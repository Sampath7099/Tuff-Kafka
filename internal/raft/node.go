// Package raft implements a simplified Raft consensus engine.
package raft

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

const (
	HeartbeatInterval  = 100 * time.Millisecond
	ElectionTimeoutMin = 300 * time.Millisecond
	ElectionTimeoutMax = 600 * time.Millisecond
)

var ErrNotLeader = errors.New("not the raft leader")

// StateMachine is called when a command is committed. Must be deterministic.
type StateMachine interface {
	Apply(cmd Command) error
}

// Transport abstracts the network for testability.
type Transport interface {
	SendRequestVote(peerID string, req RequestVoteReq) (RequestVoteResp, error)
	SendAppendEntries(peerID string, req AppendEntriesReq) (AppendEntriesResp, error)
}

type proposal struct {
	cmd    Command
	result chan error
}

// RaftNode runs the Raft consensus protocol.
type RaftNode struct {
	id        string
	peers     []string
	state     *RaftState
	sm        StateMachine
	transport Transport

	proposalCh       chan proposal
	mu               sync.Mutex
	leaderID         string
	electionTimer    *time.Timer
	stopCh           chan struct{}
	pendingProposals map[int64]chan error // logIndex → result channel
}

// NewRaftNode creates a Follower node. Call Start() to begin the event loop.
func NewRaftNode(id string, peers []string, sm StateMachine, t Transport) *RaftNode {
	n := &RaftNode{
		id:        id,
		peers:     peers,
		sm:        sm,
		transport: t,

		proposalCh:       make(chan proposal, 16),
		stopCh:           make(chan struct{}),
		pendingProposals: make(map[int64]chan error),
		state: &RaftState{
			CurrentTerm: 0,
			VotedFor:    "",
			Log:         NewRaftLog(),

			CommitIndex: 0,
			LastApplied: 0,

			NextIndex:  make(map[string]int64),
			MatchIndex: make(map[string]int64),

			Role: Follower,
		},
	}

	n.electionTimer = time.NewTimer(0)
	<-n.electionTimer.C
	return n
}

// Start begins the Raft main loop.
func (n *RaftNode) Start() {
	n.resetElectionTimer()
	go n.run()
}

// Stop shuts down the Raft main loop.
func (n *RaftNode) Stop() {
	select {
	case <-n.stopCh:
	default:
		close(n.stopCh)
	}
}

func (n *RaftNode) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state.Role == Leader
}

func (n *RaftNode) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// Propose submits a command. Blocks until committed or ctx cancelled.
func (n *RaftNode) Propose(ctx context.Context, cmd Command) error {
	n.mu.Lock()
	if n.state.Role != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	n.mu.Unlock()

	p := proposal{cmd: cmd, result: make(chan error, 1)}
	select {
	case n.proposalCh <- p:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-p.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// HandleRequestVote processes an incoming RequestVote RPC (§5.2).
func (n *RaftNode) HandleRequestVote(req RequestVoteReq) RequestVoteResp {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Rule: reject if candidate's term is stale
	if req.Term < n.state.CurrentTerm {
		return RequestVoteResp{Term: n.state.CurrentTerm, VoteGranted: false}
	}

	// Rule: if we see a higher term, step down immediately
	if req.Term > n.state.CurrentTerm {
		n.becomeFollower(req.Term)
	}

	// candidate's log must be at least as up-to-date as ours
	logOK := req.LastLogTerm > n.state.Log.LastTerm() ||
		(req.LastLogTerm == n.state.Log.LastTerm() && req.LastLogIndex >= n.state.Log.LastIndex())

	// Can only vote if we haven't voted this term, or already voted for this candidate
	canVote := n.state.VotedFor == "" || n.state.VotedFor == req.CandidateID

	if canVote && logOK {
		n.state.VotedFor = req.CandidateID
		n.resetElectionTimer()
		return RequestVoteResp{Term: n.state.CurrentTerm, VoteGranted: true}
	}

	return RequestVoteResp{Term: n.state.CurrentTerm, VoteGranted: false}
}

// HandleAppendEntries processes an incoming AppendEntries RPC (§5.3).
func (n *RaftNode) HandleAppendEntries(req AppendEntriesReq) AppendEntriesResp {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Rule: reject if leader's term is stale
	if req.Term < n.state.CurrentTerm {
		return AppendEntriesResp{Term: n.state.CurrentTerm, Success: false}
	}

	// Any valid AE (including heartbeat) means we recognize this leader
	n.becomeFollower(req.Term)
	n.leaderID = req.LeaderID

	// Check prev log consistency: follower's log is too short
	if req.PrevLogIndex > n.state.Log.LastIndex() {
		return AppendEntriesResp{
			Term:          n.state.CurrentTerm,
			Success:       false,
			ConflictIndex: n.state.Log.LastIndex() + 1,
		}
	}

	// Check prev log consistency: term mismatch at PrevLogIndex
	if n.state.Log.Entry(req.PrevLogIndex).Term != req.PrevLogTerm {
		conflictTerm := n.state.Log.Entry(req.PrevLogIndex).Term
		// Walk backwards to find the first index with this conflicting term
		// so the leader can skip the entire bad term in one round trip
		conflictIndex := req.PrevLogIndex
		for conflictIndex > 1 && n.state.Log.Entry(conflictIndex-1).Term == conflictTerm {
			conflictIndex--
		}
		n.state.Log.TruncateFrom(req.PrevLogIndex)
		return AppendEntriesResp{
			Term:          n.state.CurrentTerm,
			Success:       false,
			ConflictIndex: conflictIndex,
			ConflictTerm:  conflictTerm,
		}
	}

	// Append new entries (overwriting conflicts)
	for i, e := range req.Entries {
		idx := req.PrevLogIndex + 1 + int64(i)
		if idx <= n.state.Log.LastIndex() {
			if n.state.Log.Entry(idx).Term != e.Term {
				n.state.Log.TruncateFrom(idx)
			}
		}
		if idx > n.state.Log.LastIndex() {
			n.state.Log.Append(e)
		}
	}

	// Advance commit index to min(leaderCommit, lastLogIndex)
	if req.LeaderCommit > n.state.CommitIndex {
		newCommit := req.LeaderCommit
		if n.state.Log.LastIndex() < newCommit {
			newCommit = n.state.Log.LastIndex()
		}
		n.advanceCommit(newCommit)
	}

	return AppendEntriesResp{Term: n.state.CurrentTerm, Success: true}
}

// --- Internal methods ---

// run is the Raft main event loop.
func (n *RaftNode) run() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.electionTimer.C:
			n.startElection()
		case p := <-n.proposalCh:
			n.handleProposal(p)
		}
	}
}

// startElection transitions to Candidate, increments term, requests votes.
func (n *RaftNode) startElection() {
	n.mu.Lock()
	n.state.Role = Candidate
	n.state.CurrentTerm++
	n.state.VotedFor = n.id
	term := n.state.CurrentTerm
	lastIdx := n.state.Log.LastIndex()
	lastTerm := n.state.Log.LastTerm()
	n.mu.Unlock()

	votes := 1 // self-vote
	needed := (len(n.peers)+1)/2 + 1 // majority of total cluster (peers + self)
	var voteMu sync.Mutex

	// Single-node cluster: self-vote is already a majority
	if votes >= needed {
		n.mu.Lock()
		n.becomeLeader()
		n.mu.Unlock()
		return
	}

	for _, peer := range n.peers {
		go func(peerID string) {
			resp, err := n.transport.SendRequestVote(peerID, RequestVoteReq{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.state.CurrentTerm != term || n.state.Role != Candidate {
				return
			}
			if resp.Term > n.state.CurrentTerm {
				n.becomeFollower(resp.Term)
				return
			}
			if resp.VoteGranted {
				voteMu.Lock()
				votes++
				v := votes
				voteMu.Unlock()
				if v >= needed {
					n.becomeLeader()
				}
			}
		}(peer)
	}

	n.resetElectionTimer()
}

// becomeLeader transitions to Leader, initializes peer tracking, starts heartbeats.
// Caller must hold n.mu.
func (n *RaftNode) becomeLeader() {
	if n.state.Role == Leader {
		return
	}
	n.state.Role = Leader
	n.leaderID = n.id

	n.state.NextIndex = make(map[string]int64)
	n.state.MatchIndex = make(map[string]int64)
	for _, p := range n.peers {
		n.state.NextIndex[p] = n.state.Log.LastIndex() + 1
		n.state.MatchIndex[p] = 0
	}

	go n.sendHeartbeats()
}

// becomeFollower transitions to Follower. Caller must hold n.mu.
func (n *RaftNode) becomeFollower(term int64) {
	n.state.CurrentTerm = term
	n.state.Role = Follower
	n.state.VotedFor = ""
	n.resetElectionTimer()
}

// sendHeartbeats runs as a goroutine, replicating entries at HeartbeatInterval.
func (n *RaftNode) sendHeartbeats() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	n.replicateToAll()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			stillLeader := n.state.Role == Leader
			n.mu.Unlock()
			if !stillLeader {
				return
			}
			n.replicateToAll()
		}
	}
}

// replicateToAll sends AppendEntries to every peer in parallel.
func (n *RaftNode) replicateToAll() {
	for _, peer := range n.peers {
		go n.replicateToPeer(peer)
	}
}

// replicateToPeer sends AppendEntries to one peer and handles the response.
func (n *RaftNode) replicateToPeer(peerID string) {
	n.mu.Lock()
	if n.state.Role != Leader {
		n.mu.Unlock()
		return
	}

	nextIdx := n.state.NextIndex[peerID]
	prevLogIndex := nextIdx - 1
	prevLogTerm := n.state.Log.Entry(prevLogIndex).Term

	var entries []Entry
	if nextIdx <= n.state.Log.LastIndex() {
		entries = n.state.Log.Entries(nextIdx, n.state.Log.LastIndex()+1)
	}

	req := AppendEntriesReq{
		Term:         n.state.CurrentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.state.CommitIndex,
	}
	term := n.state.CurrentTerm
	n.mu.Unlock()

	resp, err := n.transport.SendAppendEntries(peerID, req)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state.CurrentTerm != term || n.state.Role != Leader {
		return
	}
	if resp.Term > n.state.CurrentTerm {
		n.becomeFollower(resp.Term)
		return
	}

	if resp.Success {
		n.state.NextIndex[peerID] = nextIdx + int64(len(entries))
		n.state.MatchIndex[peerID] = n.state.NextIndex[peerID] - 1
		n.checkCommit()
	} else {
		if resp.ConflictIndex > 0 {
			n.state.NextIndex[peerID] = resp.ConflictIndex
		} else if n.state.NextIndex[peerID] > 1 {
			n.state.NextIndex[peerID]--
		}
	}
}

// handleProposal appends a command to the log and begins replication.
func (n *RaftNode) handleProposal(p proposal) {
	n.mu.Lock()
	if n.state.Role != Leader {
		n.mu.Unlock()
		p.result <- ErrNotLeader
		return
	}

	payload, err := EncodeCommand(p.cmd)
	if err != nil {
		n.mu.Unlock()
		p.result <- err
		return
	}

	entry := Entry{
		Term:    n.state.CurrentTerm,
		Index:   n.state.Log.LastIndex() + 1,
		Payload: payload,
	}
	n.state.Log.Append(entry)
	n.pendingProposals[entry.Index] = p.result
	n.mu.Unlock()

	n.replicateToAll()
}

// checkCommit checks if any uncommitted entry has been replicated to a majority.
// Caller must hold n.mu.
func (n *RaftNode) checkCommit() {
	for idx := n.state.CommitIndex + 1; idx <= n.state.Log.LastIndex(); idx++ {
		// §5.4.2: only commit entries from the current term
		if n.state.Log.Entry(idx).Term != n.state.CurrentTerm {
			continue
		}

		replicas := 1 // count self
		for _, peer := range n.peers {
			if n.state.MatchIndex[peer] >= idx {
				replicas++
			}
		}

		if replicas >= len(n.peers)/2+1 {
			n.advanceCommit(idx)
			// Signal pending proposals up to committed index
			for i := n.state.CommitIndex; i >= 1; i-- {
				if ch, ok := n.pendingProposals[i]; ok {
					ch <- nil
					delete(n.pendingProposals, i)
				}
			}
		}
	}
}

// advanceCommit applies all entries from LastApplied+1 through `to`. Caller must hold n.mu.
func (n *RaftNode) advanceCommit(to int64) {
	for n.state.LastApplied < to {
		n.state.LastApplied++
		entry := n.state.Log.Entry(n.state.LastApplied)
		cmd, err := DecodeCommand(entry.Payload)
		if err == nil {
			_ = n.sm.Apply(cmd)
		}
	}
	n.state.CommitIndex = to
}

// resetElectionTimer randomizes the election timeout in [300ms, 600ms].
func (n *RaftNode) resetElectionTimer() {
	d := ElectionTimeoutMin + time.Duration(rand.Int63n(int64(ElectionTimeoutMax-ElectionTimeoutMin)))
	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(d)
	} else {
		if !n.electionTimer.Stop() {
			select {
			case <-n.electionTimer.C:
			default:
			}
		}
		n.electionTimer.Reset(d)
	}
}
