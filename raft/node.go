package raft

import (
	"errors"
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

type Raft struct {
	mu                sync.Mutex
	id                int
	state             uint32
	currentTerm       uint64     // 服务器最后知道的任期号（从0开始递增）
	votedFor          int        // 在当前任期内收到选票的候选人id（如果没有就为 null）
	logs              []LogEntry // 日志条目；每个条目包含状态机的要执行命令和从领导人处收到时的任期号
	commitIndex       int        // 已知的被提交的最大日志条目的索引值（从0开始递增）
	lastApplied       int        // 被状态机执行的最大日志条目的索引值（从0开始递增）
	nextIndexs        []int      // 对于每一个服务器，记录需要发给它的下一个日志条目的索引（初始化为领导人上一条日志的索引值+1）
	matchIndexs       []int      // 对于每一个服务器，记录已经复制到该服务器的日志的最高索引值（从0开始递增）
	peers             []RPC
	electionTimer     *time.Timer
	voteAcquired      uint32
	voteEvent         chan struct{}
	voteAcquiredEvent chan struct{}
	appendEvent       chan struct{}
	applyChan         chan ApplyMsg
}

func NewRaft(id int, peers []RPC, applyChan chan ApplyMsg) *Raft {
	rf := &Raft{
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
		applyChan:         applyChan,
	}
	go rf.loop()
	return rf
}

func (rf *Raft) ProposalCommand(command []byte) error {
	if rf.isState(Leader) {
		rf.mu.Lock()
		log := LogEntry{Term: rf.currentTerm, Command: command}
		rf.logs = append(rf.logs, log)
		index := len(rf.logs)
		rf.nextIndexs[rf.id] = index + 1
		rf.matchIndexs[rf.id] = index
		rf.mu.Unlock()
	} else {
		return errors.New("forbid proposal")
	}
	return nil
}

func randElectionDuration() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Millisecond * time.Duration(r.Int63n(MAX_ELECTION_INTERVAL-MIN_ELECTION_INTERVAL)+MIN_ELECTION_INTERVAL)
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Reset(randElectionDuration())
}

func (rf *Raft) loop() {
	ticker := time.NewTicker(HEARTBEAT_INTERVAL * time.Millisecond)
	for {
		switch atomic.LoadUint32(&rf.state) {
		case Follower:
			select {
			case <-rf.voteEvent:
				rf.resetElectionTimer()
			case <-rf.appendEvent:
				rf.resetElectionTimer()
				rf.applyLog()
			case <-rf.electionTimer.C:
				rf.resetElectionTimer()
				rf.updateStateTo(Candidate)
				rf.startElection()
			}
		case Candidate:
			select {
			case <-rf.voteAcquiredEvent:
				if atomic.LoadUint32(&rf.voteAcquired) > uint32(len(rf.peers)/2) {
					rf.updateStateTo(Leader)
					rf.broadcastAppendEntries()
				}
			case <-rf.appendEvent:
				rf.updateStateTo(Follower)
				rf.applyLog()
			case <-rf.electionTimer.C:
				rf.electionTimer.Reset(randElectionDuration())
				rf.startElection()
			}
		case Leader:
			<-ticker.C
			rf.broadcastAppendEntries()
			rf.updateCommitIndex()
			rf.applyLog()
		}
	}
}

func (rf *Raft) updateStateTo(state uint32) {
	stateDesc := []string{"FOLLOWER", "CANDIDATE", "LEADER"}
	for {
		old := atomic.LoadUint32(&rf.state)
		if old == state {
			return
		}
		if atomic.CompareAndSwapUint32(&rf.state, old, state) {
			switch state {
			case Follower:
				rf.votedFor = -1
			case Candidate:
			case Leader:
				rf.votedFor = -1
				for i := range rf.peers {
					rf.nextIndexs[i] = rf.getLastLogIndex() + 1
					rf.matchIndexs[i] = 0
				}
				atomic.StoreUint32(&rf.voteAcquired, 0)
			}

			logger.Infof("-> update state %s -> %s, vote for %d", stateDesc[old], stateDesc[state], rf.votedFor)
			return
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.id
	rf.voteAcquired = 1
	rf.mu.Unlock()

	lastLogIndex := rf.getLastLogIndex()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.logs[lastLogIndex].Term,
	}

	f := func(id int, peer RPC) {
		var reply VoteResponse
		if err := peer.Call("Raft.RequestVote", &args, &reply); err == nil {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !rf.isState(Candidate) {
				return
			}
			if reply.VoteGranted {
				rf.voteAcquired += 1
				rf.voteAcquiredEvent <- struct{}{}
			} else {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.updateStateTo(Follower)
				}
			}
		} else {
			logger.Errorf("Raft.RequestVote Peer[%d] error: %v", id, err)
		}

	}

	rf.broadcast(f)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *VoteResponse) error {
	//logger.Infof("get request vote from [%d], term [%d]", args.CandidateId, args.Term)
	rf.mu.Lock()
	rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return nil
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.updateStateTo(Follower)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			// 说明同时有多个节点发起投票
			reply.VoteGranted = false
		}
	}

	lastLogTerm := rf.logs[rf.getLastLogIndex()].Term
	if lastLogTerm > args.LastLogTerm {
		reply.VoteGranted = false
	} else if lastLogTerm == args.LastLogTerm {
		if rf.getLastLogIndex() > args.LastLogIndex {
			reply.VoteGranted = false
		}
	}

	if reply.VoteGranted == true {
		go func() {
			rf.voteEvent <- struct{}{}
		}()
	}

	return nil
}

func (rf *Raft) broadcastAppendEntries() {
	f := func(id int, peer RPC) {
		var args AppendEntriesArgs
		rf.mu.Lock()
		args.Term = rf.currentTerm
		args.LeaderId = rf.id
		args.LeaderCommit = rf.commitIndex
		args.PrevLogIndex = rf.nextIndexs[id] - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		if rf.getLastLogIndex() >= rf.nextIndexs[id] {
			args.Entries = rf.logs[rf.nextIndexs[id]:]
		}
		rf.mu.Unlock()
		var reply AppendEntriesReply
		if err := peer.Call("Raft.AppendEntries", &args, &reply); err == nil {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !rf.isState(Leader) {
				return
			}
			if reply.Success {
				rf.nextIndexs[id] += len(args.Entries)
				rf.matchIndexs[id] = rf.nextIndexs[id] - 1
			} else {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.updateStateTo(Follower)
				}
			}
		} else {
			logger.Errorf("Raft.AppendEntries Peer[%d] error: %v", id, err)
		}
	}

	rf.broadcast(f)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	//logger.Infof("get append entries from [%d], term [%d]", args.LeaderId, args.Term)
	defer func() {
		rf.appendEvent <- struct{}{}
	}()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else if args.Term > rf.currentTerm {
		reply.Success = true
		rf.currentTerm = args.Term
		rf.updateStateTo(Follower)
	} else {
		reply.Success = true
	}

	// 如果在prevLogIndex处的日志的任期号与prevLogTerm不匹配时，返回 false（5.3节）
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		return nil
	}

	if args.PrevLogTerm != rf.logs[args.PrevLogTerm].Term {
		reply.Success = false
		return nil
	}

	// 如果一条已经存在的日志与新的冲突（index 相同但是任期号 term 不同），则删除已经存在的日志和它之后所有的日志（5.3节）
	conflict := -1
	if rf.getLastLogIndex() < args.PrevLogIndex+len(args.Entries) {
		conflict = args.PrevLogIndex + 1
	} else {
		for idx := 0; idx < len(args.Entries); idx++ {
			if rf.logs[idx+args.PrevLogIndex+1].Term != args.Entries[idx].Term {
				conflict = idx + args.PrevLogIndex + 1
				break
			}
		}
	}
	// 添加任何在已有的日志中不存在的条目
	if conflict != -1 {
		rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
	}
	// 如果leaderCommit > commitIndex，将commitIndex设置为leaderCommit和最新日志条目索引号中较小的一个
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.getLastLogIndex() {
			rf.commitIndex = rf.getLastLogIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	return nil
}
func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.getLastLogIndex(); i > rf.commitIndex; i-- {
		match := 1
		for i, matched := range rf.matchIndexs {
			if i == rf.id {
				continue
			}
			if matched > rf.commitIndex {
				match++
			}
		}
		if match > len(rf.peers)/2 {
			rf.commitIndex = i
		}
	}

}

func (rf *Raft) broadcast(f func(id int, peer RPC)) {
	for index, peer := range rf.peers {
		if index == rf.id {
			continue
		}
		go f(index, peer)
	}
}

func (rf *Raft) isState(state uint32) bool {
	return atomic.LoadUint32(&rf.state) == state
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			var msg ApplyMsg
			msg.Index = i
			msg.Command = rf.logs[i].Command
			rf.applyChan <- msg
			rf.lastApplied = i
			logger.Infof("commit command:%s", string(msg.Command))
		}
	}
}
