package raft

import (
	"github.com/snlansky/glibs/logging"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var logger = logging.MustGetLogger("raft")

const (
	Follower uint32 = iota
	Candidate
	Leader

	HEARTBEAT_INTERVAL    = 1000
	MIN_ELECTION_INTERVAL = 4000
	MAX_ELECTION_INTERVAL = 5000
)

type Node struct {
	isLeader bool
	kv       map[string]string
	mu       sync.RWMutex
}

func NewNode() *Node {
	n := Node{
		isLeader: false,
		kv:       map[string]string{},
	}
	return &n
}

func (n *Node) Set(key, value string) {
	n.mu.Lock()
	n.kv[key] = value
	n.mu.Unlock()
}

func (n *Node) Get(key string) string {
	n.mu.RLock()
	value, ok := n.kv[key]
	n.mu.RUnlock()
	if !ok {
		return ""
	}
	return value
}

func (n *Node) Delete(key string) {
	n.mu.Lock()
	delete(n.kv, key)
	n.mu.Unlock()
}

type Raft struct {
	mu                sync.Mutex
	id                int
	state             uint32
	currentTerm       uint64     // 服务器最后知道的任期号（从0开始递增）
	votedFor          int        // 在当前任期内收到选票的候选人id（如果没有就为 null）
	logs              []LogEntry // 日志条目；每个条目包含状态机的要执行命令和从领导人处收到时的任期号
	commitIndex       int        // 已知的被提交的最大日志条目的索引值（从0开始递增）
	lastApplied       uint64     // 被状态机执行的最大日志条目的索引值（从0开始递增）
	nextIndexs        []int      // 对于每一个服务器，记录需要发给它的下一个日志条目的索引（初始化为领导人上一条日志的索引值+1）
	matchIndexs       []int      // 对于每一个服务器，记录已经复制到该服务器的日志的最高索引值（从0开始递增）
	peers             []RPC
	electionTimer     *time.Timer
	voteAcquired      uint32
	voteEvent         chan struct{}
	voteAcquiredEvent chan struct{}
	appendEvent       chan struct{}
}

func NewRaft(id int, peers []RPC) *Raft {
	r := &Raft{
		mu:                sync.Mutex{},
		id:                id,
		state:             Follower,
		currentTerm:       0,
		votedFor:          0,
		logs:              make([]LogEntry, 1),
		commitIndex:       0,
		lastApplied:       0,
		nextIndexs:        make([]int, len(peers)),
		matchIndexs:       make([]int, len(peers)),
		peers:             peers,
		electionTimer:     time.NewTimer(randElectionDuration()),
		voteAcquired:      0,
		voteEvent:         make(chan struct{}),
		voteAcquiredEvent: make(chan struct{}),
		appendEvent:       make(chan struct{}),
	}
	go r.loop()
	return r
}

func randElectionDuration() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Millisecond * time.Duration(r.Int63n(MAX_ELECTION_INTERVAL-MIN_ELECTION_INTERVAL)+MIN_ELECTION_INTERVAL)
}

func (r *Raft) resetElectionTimer() {
	r.electionTimer.Reset(randElectionDuration())
}

func (r *Raft) loop() {
	ticker := time.NewTicker(HEARTBEAT_INTERVAL * time.Millisecond)
	for {
		switch atomic.LoadUint32(&r.state) {
		case Follower:
			select {
			case <-r.voteEvent:
				r.resetElectionTimer()
			case <-r.appendEvent:
				r.resetElectionTimer()
			case <-r.electionTimer.C:
				r.resetElectionTimer()
				r.updateStateTo(Candidate)
				r.startElection()
			}
		case Candidate:
			select {
			case <-r.voteAcquiredEvent:
				if atomic.LoadUint32(&r.voteAcquired) > uint32(len(r.peers)/2) {
					r.updateStateTo(Leader)
					r.broadcastAppendEntries()
				}
			case <-r.appendEvent:
				r.updateStateTo(Follower)
			case <-r.electionTimer.C:
				r.electionTimer.Reset(randElectionDuration())
				r.startElection()
			}
		case Leader:
			<-ticker.C
			r.broadcastAppendEntries()
			r.updateCommitIndex()
		}
	}
}

func (r *Raft) updateStateTo(state uint32) {
	stateDesc := []string{"FOLLOWER", "CANDIDATE", "LEADER"}
	for {
		old := atomic.LoadUint32(&r.state)
		if old == state {
			return
		}
		if atomic.CompareAndSwapUint32(&r.state, old, state) {
			switch state {
			case Follower:
				r.votedFor = -1
			case Candidate:
			case Leader:
				r.votedFor = -1
				for i := range r.peers {
					r.nextIndexs[i] = r.getLastLogIndex() + 1
					r.matchIndexs[i] = 0
				}
				atomic.StoreUint32(&r.voteAcquired, 0)
			}

			logger.Infof("-> update state %s -> %s, vote for %d", stateDesc[old], stateDesc[state], r.votedFor)
			return
		}
	}
}

func (r *Raft) startElection() {
	r.mu.Lock()
	r.currentTerm += 1
	r.votedFor = r.id
	r.voteAcquired = 1
	r.mu.Unlock()

	lastLogIndex := r.getLastLogIndex()
	args := RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateId:  r.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  r.logs[lastLogIndex].Term,
	}

	f := func(id int, peer RPC) {
		var reply VoteResponse
		if err := peer.Call("Raft.RequestVote", &args, &reply); err == nil {
			r.mu.Lock()
			defer r.mu.Unlock()
			if !r.isState(Candidate) {
				return
			}
			if reply.VoteGranted {
				r.voteAcquired += 1
				r.voteAcquiredEvent <- struct{}{}
			} else {
				if reply.Term > r.currentTerm {
					r.currentTerm = reply.Term
					r.updateStateTo(Follower)
				}
			}
		} else {
			logger.Errorf("Raft.RequestVote Peer[%d] error: %v", id, err)
		}

	}

	r.broadcast(f)
}

func (r *Raft) RequestVote(args *RequestVoteArgs, reply *VoteResponse) error {
	//logger.Infof("get request vote from [%d], term [%d]", args.CandidateId, args.Term)
	r.mu.Lock()
	r.mu.Unlock()

	reply.Term = r.currentTerm
	reply.VoteGranted = false

	if args.Term < r.currentTerm {
		return nil
	} else if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.updateStateTo(Follower)
		r.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		if r.votedFor == -1 {
			r.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			// 说明同时有多个节点发起投票
			reply.VoteGranted = false
		}
	}

	lastLogTerm := r.logs[r.getLastLogIndex()].Term
	if lastLogTerm > args.LastLogTerm {
		reply.VoteGranted = false
	} else if lastLogTerm == args.LastLogTerm {
		if r.getLastLogIndex() > args.LastLogIndex {
			reply.VoteGranted = false
		}
	}

	if reply.VoteGranted == true {
		go func() {
			r.voteEvent <- struct{}{}
		}()
	}

	return nil
}

func (r *Raft) broadcastAppendEntries() {
	f := func(id int, peer RPC) {
		var args AppendEntriesArgs
		r.mu.Lock()
		args.Term = r.currentTerm
		args.LeaderId = r.id
		args.LeaderCommit = r.commitIndex
		args.PrevLogIndex = r.nextIndexs[id] - 1
		args.PrevLogTerm = r.logs[args.PrevLogIndex].Term
		if r.getLastLogIndex() >= r.nextIndexs[id] {
			args.Entries = r.logs[r.nextIndexs[id]:]
		}
		r.mu.Unlock()
		var reply AppendEntriesReply
		if err := peer.Call("Raft.AppendEntries", &args, &reply); err == nil {
			r.mu.Lock()
			defer r.mu.Unlock()
			if !r.isState(Leader) {
				return
			}
			if reply.Success {
				r.nextIndexs[id] += len(args.Entries)
				r.matchIndexs[id] = r.nextIndexs[id] - 1
			} else {
				if reply.Term > r.currentTerm {
					r.currentTerm = reply.Term
					r.updateStateTo(Follower)
				}
			}
		} else {
			logger.Errorf("Raft.AppendEntries Peer[%d] error: %v", id, err)
		}
	}

	r.broadcast(f)
}

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	//logger.Infof("get append entries from [%d], term [%d]", args.LeaderId, args.Term)
	defer func() {
		r.appendEvent <- struct{}{}
	}()
	r.mu.Lock()
	defer r.mu.Unlock()
	if args.Term < r.currentTerm {
		reply.Success = false
		reply.Term = r.currentTerm
	} else if args.Term > r.currentTerm {
		reply.Success = true
		r.currentTerm = args.Term
		r.updateStateTo(Follower)
	} else {
		reply.Success = true
	}

	// 如果在prevLogIndex处的日志的任期号与prevLogTerm不匹配时，返回 false（5.3节）
	if args.PrevLogIndex > r.getLastLogIndex() {
		reply.Success = false
		return nil
	}

	if args.PrevLogTerm != r.logs[args.PrevLogTerm].Term {
		reply.Success = false
		return nil
	}

	// 如果一条已经存在的日志与新的冲突（index 相同但是任期号 term 不同），则删除已经存在的日志和它之后所有的日志（5.3节）
	conflict := -1
	if r.getLastLogIndex() < args.PrevLogIndex+len(args.Entries) {
		conflict = args.PrevLogIndex + 1
	} else {
		for idx := 0; idx < len(args.Entries); idx++ {
			if r.logs[idx+args.PrevLogIndex+1].Term != args.Entries[idx].Term {
				conflict = idx + args.PrevLogIndex + 1
				break
			}
		}
	}
	// 添加任何在已有的日志中不存在的条目
	if conflict != -1 {
		r.logs = append(r.logs[:args.PrevLogIndex+1], args.Entries...)
	}
	// 如果leaderCommit > commitIndex，将commitIndex设置为leaderCommit和最新日志条目索引号中较小的一个
	if args.LeaderCommit > r.commitIndex {
		if args.LeaderCommit < r.getLastLogIndex() {
			r.commitIndex = r.getLastLogIndex()
		} else {
			r.commitIndex = args.LeaderCommit
		}
	}
	return nil
}
func (r *Raft) updateCommitIndex() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := r.getLastLogIndex(); i > r.commitIndex; i-- {
		match := 1
		for i, matched := range r.matchIndexs {
			if i == r.id {
				continue
			}
			if matched > r.commitIndex {
				match++
			}
		}
		if match > len(r.peers)/2 {
			r.commitIndex = i
		}
	}

}

func (r *Raft) broadcast(f func(id int, peer RPC)) {
	for index, peer := range r.peers {
		if index == r.id {
			continue
		}
		go f(index, peer)
	}
}

func (r *Raft) isState(state uint32) bool {
	return atomic.LoadUint32(&r.state) == state
}

func (r *Raft) getLastLogIndex() int {
	return len(r.logs) - 1
}

//func (r *Raft) InstallSnapshotRPC(s *Snapshot) *InstallSnapshotResponse {
//	resp := new(InstallSnapshotResponse)
//	resp.Term = r.currentTerm
//
//	if s.Term < r.currentTerm {
//		return resp
//	}
//
//}
