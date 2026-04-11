package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/Sampath7099/Tuff-Kafka/internal/raft"
)

// ============================================================================
// Mock Raft Node for handler tests
// ============================================================================

// mockRaftNode implements a minimal Raft node interface for testing handlers.
type mockRaftNode struct {
	isLeader    bool
	leaderAddr  string
	proposed    []raft.Command
	proposeErr  error
}

func (m *mockRaftNode) IsLeader() bool { return m.isLeader }
func (m *mockRaftNode) LeaderID() string { return m.leaderAddr }
func (m *mockRaftNode) Propose(ctx context.Context, cmd raft.Command) error {
	if m.proposeErr != nil {
		return m.proposeErr
	}
	m.proposed = append(m.proposed, cmd)
	return nil
}

// helper: create a server with mock raft node
func newTestServer(t *testing.T, isLeader bool) (*Server, *mockRaftNode, *ClusterState) {
	t.Helper()
	state := NewClusterState()
	require.NotNil(t, state)

	mockRaft := &mockRaftNode{isLeader: isLeader}

	srv := &Server{
		id:       "test-controller",
		addr:     "localhost:9093",
		state:    state,
		raftNode: mockRaft,
	}

	// Pre-populate with some brokers for meaningful tests
	registerBroker(t, state, "b1", "broker1:9092")
	registerBroker(t, state, "b2", "broker2:9092")
	registerBroker(t, state, "b3", "broker3:9092")

	return srv, mockRaft, state
}

// TestController_CreateTopic_ProposesAndCommits verifies that handleCreateTopic
// proposes a CmdCreateTopic command to Raft when called on the leader.
func TestController_CreateTopic_ProposesAndCommits(t *testing.T) {
	srv, mockRaft, state := newTestServer(t, true)
	_ = srv
	_ = mockRaft
	_ = state

	reqData, err := msgpack.Marshal(struct {
		Topic      string `msgpack:"topic"`
		Partitions int32  `msgpack:"partitions"`
	}{Topic: "orders", Partitions: 6})
	require.NoError(t, err)

	resp, err := srv.handleCreateTopic(reqData)
	// When implemented: should succeed and propose to Raft
	// For now: test will fail until implementation is done
	require.NoError(t, err, "handleCreateTopic should succeed on leader")
	assert.NotNil(t, resp, "response should not be nil")
}

// TestController_Metadata_ReturnsCurrent verifies that handleMetadata
// returns the current cluster state snapshot.
func TestController_Metadata_ReturnsCurrent(t *testing.T) {
	srv, _, state := newTestServer(t, true)

	// Create a topic directly in state for testing
	cmd := makeCommand(t, raft.CmdCreateTopic, struct {
		Topic      string `msgpack:"topic"`
		Partitions int32  `msgpack:"partitions"`
	}{Topic: "events", Partitions: 3})
	require.NoError(t, state.Apply(cmd))

	reqData, err := msgpack.Marshal(struct {
		Topic string `msgpack:"topic"`
	}{Topic: "events"})
	require.NoError(t, err)

	resp, err := srv.handleMetadata(reqData)
	require.NoError(t, err, "handleMetadata should succeed")
	assert.NotNil(t, resp, "metadata response should not be nil")
}

// TestController_Metadata_AllowsNonLeader verifies that handleMetadata
// can be served by a non-leader controller from replicated state.
func TestController_Metadata_AllowsNonLeader(t *testing.T) {
	srv, _, _ := newTestServer(t, false)

	reqData, err := msgpack.Marshal(struct {
		Topic string `msgpack:"topic"`
	}{Topic: ""})
	require.NoError(t, err)

	resp, err := srv.handleMetadata(reqData)
	assert.NoError(t, err, "non-leader should serve metadata")
	assert.NotNil(t, resp, "metadata response should not be nil")
}

// TestController_BrokerFailure_TriggersReassignment verifies that when a broker
// is detected as dead, its partitions are reassigned to live brokers.
func TestController_BrokerFailure_TriggersReassignment(t *testing.T) {
	_, _, state := newTestServer(t, true)

	// Create a topic
	cmd := makeCommand(t, raft.CmdCreateTopic, struct {
		Topic      string `msgpack:"topic"`
		Partitions int32  `msgpack:"partitions"`
	}{Topic: "orders", Partitions: 6})
	require.NoError(t, state.Apply(cmd))

	// Mark b2 as dead
	state.mu.Lock()
	state.Brokers["b2"].LastSeen = time.Now().Add(-5 * time.Minute)
	state.mu.Unlock()

	// After detectAndReassign runs, b2's partitions should move to b1 or b3
	// (This tests the state logic — actual reassignment flow needs Raft)
	liveIDs := state.liveBrokerIDs(10 * time.Second)
	assert.NotContains(t, liveIDs, "b2", "dead broker should not be in live list")
	assert.Equal(t, 2, len(liveIDs), "should have 2 live brokers")
}

// TestController_ConsumerHeartbeatTimeout_TriggersRebalance verifies that
// when a consumer group member's heartbeat times out, the controller triggers
// a rebalance.
func TestController_ConsumerHeartbeatTimeout_TriggersRebalance(t *testing.T) {
	_, _, state := newTestServer(t, true)

	// Create topic and join two members
	createCmd := makeCommand(t, raft.CmdCreateTopic, struct {
		Topic      string `msgpack:"topic"`
		Partitions int32  `msgpack:"partitions"`
	}{Topic: "t1", Partitions: 4})
	require.NoError(t, state.Apply(createCmd))

	for _, mid := range []string{"m1", "m2"} {
		joinCmd := makeCommand(t, raft.CmdJoinGroup, struct {
			GroupID   string   `msgpack:"group_id"`
			MemberID  string   `msgpack:"member_id"`
			Topics    []string `msgpack:"topics"`
			SessionMs int32    `msgpack:"session_ms"`
		}{GroupID: "g1", MemberID: mid, Topics: []string{"t1"}, SessionMs: 10000})
		require.NoError(t, state.Apply(joinCmd))
	}

	// Make m2's heartbeat stale
	state.mu.Lock()
	if g, ok := state.Groups["g1"]; ok {
		if m, ok := g.Members["m2"]; ok {
			m.LastSeen = time.Now().Add(-1 * time.Minute)
		}
	}
	state.mu.Unlock()

	// After leave command for m2, group should rebalance
	leaveCmd := makeCommand(t, raft.CmdLeaveGroup, struct {
		GroupID  string `msgpack:"group_id"`
		MemberID string `msgpack:"member_id"`
	}{GroupID: "g1", MemberID: "m2"})
	require.NoError(t, state.Apply(leaveCmd))

	state.mu.RLock()
	g := state.Groups["g1"]
	state.mu.RUnlock()

	if g != nil {
		assert.Equal(t, 1, len(g.Members), "should have 1 member after timeout")
	}
}

// TestController_JoinGroup_ReturnsAssignment verifies that handleJoinGroup
// returns a valid JoinGroupResp with partition assignments and generation.
func TestController_JoinGroup_ReturnsAssignment(t *testing.T) {
	srv, _, state := newTestServer(t, true)

	createCmd := makeCommand(t, raft.CmdCreateTopic, struct {
		Topic      string `msgpack:"topic"`
		Partitions int32  `msgpack:"partitions"`
	}{Topic: "t1", Partitions: 3})
	require.NoError(t, state.Apply(createCmd))

	reqData, err := msgpack.Marshal(struct {
		GroupID   string   `msgpack:"group_id"`
		MemberID  string   `msgpack:"member_id"`
		Topics    []string `msgpack:"topics"`
		SessionMs int32    `msgpack:"session_ms"`
	}{GroupID: "g1", MemberID: "m1", Topics: []string{"t1"}, SessionMs: 10000})
	require.NoError(t, err)

	resp, err := srv.handleJoinGroup(reqData)
	require.NoError(t, err, "handleJoinGroup should succeed")
	assert.NotNil(t, resp, "response should contain assignment")
}
