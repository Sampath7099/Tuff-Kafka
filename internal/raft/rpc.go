package raft

// RequestVoteReq is the Raft RequestVote RPC (§5.2).
type RequestVoteReq struct {
	Term         int64  `msgpack:"term"`
	CandidateID  string `msgpack:"candidate_id"`
	LastLogIndex int64  `msgpack:"last_log_index"`
	LastLogTerm  int64  `msgpack:"last_log_term"`
}

type RequestVoteResp struct {
	Term        int64 `msgpack:"term"`
	VoteGranted bool  `msgpack:"vote_granted"`
}

// AppendEntriesReq is the Raft AppendEntries RPC (§5.3).
// Empty Entries = heartbeat.
type AppendEntriesReq struct {
	Term         int64   `msgpack:"term"`
	LeaderID     string  `msgpack:"leader_id"`
	PrevLogIndex int64   `msgpack:"prev_log_index"`
	PrevLogTerm  int64   `msgpack:"prev_log_term"`
	Entries      []Entry `msgpack:"entries"`
	LeaderCommit int64   `msgpack:"leader_commit"`
}

type AppendEntriesResp struct {
	Term          int64 `msgpack:"term"`
	Success       bool  `msgpack:"success"`
	ConflictIndex int64 `msgpack:"conflict_index"` // fast backtrack optimization
	ConflictTerm  int64 `msgpack:"conflict_term"`
}
