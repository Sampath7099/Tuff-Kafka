package raft

import "sync"

// Role represents the three possible states of a Raft node.
type Role uint8

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2
)

// RaftState holds persistent and volatile Raft state per the Raft paper §5.
type RaftState struct {
	mu sync.RWMutex

	// Persistent (must survive crash)
	CurrentTerm int64
	VotedFor    string   // empty = no vote cast this term
	Log         *RaftLog

	// Volatile (all nodes)
	CommitIndex int64
	LastApplied int64

	// Volatile (leader only, reinitialized after election)
	NextIndex  map[string]int64 // peerID → next log index to send
	MatchIndex map[string]int64 // peerID → highest replicated index

	Role Role
}
