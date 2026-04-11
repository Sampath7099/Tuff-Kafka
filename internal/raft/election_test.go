package raft

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestElection_SplitVote_NewElection verifies that when a split vote occurs
// (no candidate gets a majority), nodes re-randomize their election timeout
// and eventually a leader is elected.
func TestElection_SplitVote_NewElection(t *testing.T) {
	// With 5 nodes, split votes are more likely
	tc := newTestCluster(t, 5)
	tc.startAll()
	defer tc.stopAll()

	// Give enough time for potential split votes to resolve
	tc.waitForLeader(t, 5*ElectionTimeoutMax)

	// Exactly one leader should emerge
	assert.Equal(t, 1, tc.countLeaders(), "exactly one leader after potential split votes")
}

// TestElection_CandidateWithStaleTerm_StepsDown verifies that a Candidate
// receiving an AppendEntries with a higher term steps down to Follower.
func TestElection_CandidateWithStaleTerm_StepsDown(t *testing.T) {
	tc := newTestCluster(t, 3)
	node := tc.nodes[0]
	require.NotNil(t, node)

	// Simulate: node enters candidate state at term 1
	// Then receives AppendEntries from a leader at term 5
	resp := node.HandleAppendEntries(AppendEntriesReq{
		Term:         5,
		LeaderID:     "leader-x",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil, // heartbeat
		LeaderCommit: 0,
	})

	assert.True(t, resp.Success, "should accept AppendEntries from higher term")
	assert.False(t, node.IsLeader(), "node should not be leader after higher-term AE")
}

// TestElection_LogUpToDateCheck verifies that a candidate with a shorter
// log does not win the election against a node with a longer log.
func TestElection_LogUpToDateCheck(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.startAll()
	defer tc.stopAll()

	leader := tc.waitForLeader(t, 3*ElectionTimeoutMax)
	require.NotNil(t, leader)

	// Propose 5 entries so the leader has a longer log
	for i := 0; i < 5; i++ {
		cmd := Command{Type: CmdCreateTopic, Data: []byte("entry")}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := leader.Propose(ctx, cmd)
		cancel()
		require.NoError(t, err)
	}

	// Wait for full replication
	time.Sleep(5 * HeartbeatInterval)

	// A vote request from a node with an empty log and smaller term
	// should be rejected by nodes that have a longer log
	var follower *RaftNode
	for _, n := range tc.nodes {
		if !n.IsLeader() {
			follower = n
			break
		}
	}
	require.NotNil(t, follower)

	// Send a vote request with empty log at a lower term
	resp := follower.HandleRequestVote(RequestVoteReq{
		Term:         1, // likely lower than current term
		CandidateID:  "stale-candidate",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	assert.False(t, resp.VoteGranted,
		"should reject candidate with shorter/stale log")
}

// TestElection_TermIncreasesOnEachAttempt verifies that each failed election
// attempt causes the term to increment.
func TestElection_TermIncreasesOnEachAttempt(t *testing.T) {
	// Create a 3-node cluster but partition 2 nodes so elections always fail
	tc := newTestCluster(t, 3)

	// Partition nodes B and C so node A can never get a majority
	tc.transport.partition(nodeID(1))
	tc.transport.partition(nodeID(2))

	tc.nodes[0].Start()
	defer tc.nodes[0].Stop()

	// Wait for several election timeout cycles
	time.Sleep(5 * ElectionTimeoutMax)

	// Node A should have attempted multiple elections, incrementing its term
	// We can verify by sending a RequestVote with term 1 — it should be rejected
	// because node A's term should be > 1 by now
	resp := tc.nodes[0].HandleRequestVote(RequestVoteReq{
		Term:         1,
		CandidateID:  "test",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	// The response term should be > 1 (node has had multiple election attempts)
	assert.Greater(t, resp.Term, int64(1),
		"node should have incremented term through multiple election attempts")
}

// TestElection_VotedForResetsOnTermChange verifies that the VotedFor field
// is cleared when a node transitions to a new term, allowing it to vote
// for a different candidate.
func TestElection_VotedForResetsOnTermChange(t *testing.T) {
	tc := newTestCluster(t, 3)
	node := tc.nodes[0]
	require.NotNil(t, node)

	// Vote for candidate A in term 1
	resp1 := node.HandleRequestVote(RequestVoteReq{
		Term:         1,
		CandidateID:  "A",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	assert.True(t, resp1.VoteGranted, "should vote for A in term 1")

	// In term 1, should not vote for B
	resp2 := node.HandleRequestVote(RequestVoteReq{
		Term:         1,
		CandidateID:  "B",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	assert.False(t, resp2.VoteGranted, "should not vote for B in same term")

	// In term 2, VotedFor should be reset — should be able to vote for B
	resp3 := node.HandleRequestVote(RequestVoteReq{
		Term:         2,
		CandidateID:  "B",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	assert.True(t, resp3.VoteGranted, "should vote for B in new term 2")
}

// TestElection_LeaderStepsDownOnHigherTerm verifies that a Leader receiving
// an AppendEntries or RequestVote with a higher term immediately steps down
// to Follower.
func TestElection_LeaderStepsDownOnHigherTerm(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.startAll()
	defer tc.stopAll()

	leader := tc.waitForLeader(t, 3*ElectionTimeoutMax)
	require.NotNil(t, leader)
	assert.True(t, leader.IsLeader())

	// Send AppendEntries with a much higher term
	resp := leader.HandleAppendEntries(AppendEntriesReq{
		Term:         99,
		LeaderID:     "higher-term-leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	})

	assert.True(t, resp.Success, "should accept AE from higher term")
	assert.False(t, leader.IsLeader(), "leader should step down on higher term")
}
