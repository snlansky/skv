package raft

type RPC interface {
	Call(serviceMethod string, args interface{}, reply interface{}) error
}

type Proposal interface {
	ProposalCommand(command []byte) error
}

type LogEntry struct {
	Term    uint64
	Command []byte
}

type AppendEntriesArgs struct {
	Term         uint64     // 领导人的任期号
	LeaderId     int        // 领导人的 id，为了其他服务器能重定向到客户端
	PrevLogIndex int        // 最新日志之前的日志的索引值
	PrevLogTerm  uint64     // 最新日志之前的日志的领导人任期号
	Entries      []LogEntry // 将要存储的日志条目（表示 heartbeat 时为空，有时会为了效率发送超过一条）
	LeaderCommit int        // 领导人提交的日志条目索引值
}

type AppendEntriesReply struct {
	Term    uint64 // 当前的任期号，用于领导人更新自己的任期号
	Success bool   // 如果其它服务器包含能够匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
}

type RequestVoteArgs struct {
	Term         uint64 // 候选人的任期号
	CandidateId  int    // 请求投票的候选人 id
	LastLogIndex int    // 候选人最新日志条目的索引值
	LastLogTerm  uint64 // 候选人最新日志条目对应的任期号
}

type VoteResponse struct {
	Term        uint64 // 目前的任期号，用于候选人更新自己
	VoteGranted bool   // 如果候选人收到选票为 true
}

type Snapshot struct {
	Term              uint64 // 领导人的任期
	LeaderId          uint64 // 为了追随者能重定向到客户端
	LastIncludedIndex uint64 // 快照中包含的最后日志条目的索引值
	LastIncludedTerm  uint64 // 快照中包含的最后日志条目的任期号
	Offset            uint64 // 分块在快照中的偏移量
	Data              []byte // 快照块的原始数据
	Done              bool   // 如果是最后一块数据则为真
}

type InstallSnapshotResponse struct {
	Term uint64 // currentTerm，用于领导人更新自己
}

type Message struct {
	Type int
	Data []byte
}

type MessageType int

type ApplyMsg struct {
	Index   int
	Command []byte
}
