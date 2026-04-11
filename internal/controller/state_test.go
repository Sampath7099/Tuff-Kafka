package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/Sampath7099/Tuff-Kafka/internal/raft"
)

// helper: create a command with the given type and data struct
func makeCommand(t *testing.T, cmdType raft.CommandType, data interface{}) raft.Command {
	t.Helper()
	encoded, err := msgpack.Marshal(data)
	require.NoError(t, err)
	return raft.Command{Type: cmdType, Data: encoded}
}

// helper: register a broker in the cluster state
func registerBroker(t *testing.T, s *ClusterState, id, addr string) {
	t.Helper()
	cmd := makeCommand(t, raft.CmdRegisterBroker, struct {
		ID   string `msgpack:"id"`
		Addr string `msgpack:"addr"`
	}{ID: id, Addr: addr})
	err := s.Apply(cmd)
	require.NoError(t, err)
	// Ensure LastSeen is recent so broker is considered "live"
	s.mu.Lock()
	if b, ok := s.Brokers[id]; ok {
		b.LastSeen = time.Now()
	}
	s.mu.Unlock()
}

// TestClusterState_RegisterBroker verifies that registering a broker
// adds it to the Brokers map with correct ID and address.
func TestClusterState_RegisterBroker(t *testing.T) {
	s := NewClusterState()
	require.NotNil(t, s)

	registerBroker(t, s, "broker-1", "10.0.0.1:9092")

	s.mu.RLock()
	defer s.mu.RUnlock()

	b, ok := s.Brokers["broker-1"]
	require.True(t, ok, "broker should be registered")
	assert.Equal(t, "broker-1", b.ID)
	assert.Equal(t, "10.0.0.1:9092", b.Addr)
}

// TestClusterState_CreateTopic_RoundRobinAssignment verifies that creating
// a topic with 6 partitions across 3 brokers results in 2 partitions per broker.
func TestClusterState_CreateTopic_RoundRobinAssignment(t *testing.T) {
	s := NewClusterState()
	require.NotNil(t, s)

	registerBroker(t, s, "b1", "addr1")
	registerBroker(t, s, "b2", "addr2")
	registerBroker(t, s, "b3", "addr3")

	cmd := makeCommand(t, raft.CmdCreateTopic, struct {
		Topic      string `msgpack:"topic"`
		Partitions int32  `msgpack:"partitions"`
	}{Topic: "orders", Partitions: 6})

	err := s.Apply(cmd)
	require.NoError(t, err)

	s.mu.RLock()
	defer s.mu.RUnlock()

	topic, ok := s.Topics["orders"]
	require.True(t, ok, "topic should exist")
	assert.Equal(t, int32(6), topic.Partitions)
	assert.Equal(t, 6, len(topic.Leaders), "should have 6 partition leaders")

	// Count partitions per broker
	counts := make(map[string]int)
	for _, brokerID := range topic.Leaders {
		counts[brokerID]++
	}
	// Each broker should have exactly 2 partitions
	for brokerID, count := range counts {
		assert.Equal(t, 2, count, "broker %s should have 2 partitions", brokerID)
	}
}

// TestClusterState_CreateTopic_Idempotent verifies that creating the same
// topic twice does not return an error and does not change the existing topic.
func TestClusterState_CreateTopic_Idempotent(t *testing.T) {
	s := NewClusterState()
	require.NotNil(t, s)

	registerBroker(t, s, "b1", "addr1")

	cmd := makeCommand(t, raft.CmdCreateTopic, struct {
		Topic      string `msgpack:"topic"`
		Partitions int32  `msgpack:"partitions"`
	}{Topic: "events", Partitions: 3})

	err1 := s.Apply(cmd)
	require.NoError(t, err1)

	s.mu.RLock()
	v1 := s.MetadataVersion
	s.mu.RUnlock()

	// Create same topic again
	err2 := s.Apply(cmd)
	require.NoError(t, err2, "idempotent create should not error")

	s.mu.RLock()
	v2 := s.MetadataVersion
	s.mu.RUnlock()

	assert.Equal(t, v1, v2, "metadata version should not change on idempotent create")
}

// TestClusterState_CreateTopic_NoBrokers_ReturnsError verifies that creating
// a topic with no live brokers returns an error.
func TestClusterState_CreateTopic_NoBrokers_ReturnsError(t *testing.T) {
	s := NewClusterState()
	require.NotNil(t, s)

	cmd := makeCommand(t, raft.CmdCreateTopic, struct {
		Topic      string `msgpack:"topic"`
		Partitions int32  `msgpack:"partitions"`
	}{Topic: "orphan", Partitions: 3})

	err := s.Apply(cmd)
	assert.Error(t, err, "should error when no brokers available")
}

// TestClusterState_LiveBrokerIDs_ExcludesStale verifies that brokers
// whose LastSeen is older than the timeout are excluded from liveBrokerIDs.
func TestClusterState_LiveBrokerIDs_ExcludesStale(t *testing.T) {
	s := NewClusterState()
	require.NotNil(t, s)

	registerBroker(t, s, "alive", "addr1")
	registerBroker(t, s, "dead", "addr2")

	// Make "dead" broker stale
	s.mu.Lock()
	s.Brokers["dead"].LastSeen = time.Now().Add(-1 * time.Minute)
	s.mu.Unlock()

	ids := s.liveBrokerIDs(10 * time.Second)
	assert.Contains(t, ids, "alive", "alive broker should be included")
	assert.NotContains(t, ids, "dead", "stale broker should be excluded")
}

// TestClusterState_JoinGroup_SingleMember verifies that a single member
// joining a group gets all partitions of subscribed topics.
func TestClusterState_JoinGroup_SingleMember(t *testing.T) {
	s := NewClusterState()
	require.NotNil(t, s)

	registerBroker(t, s, "b1", "addr1")

	// Create topic with 4 partitions
	createCmd := makeCommand(t, raft.CmdCreateTopic, struct {
		Topic      string `msgpack:"topic"`
		Partitions int32  `msgpack:"partitions"`
	}{Topic: "t1", Partitions: 4})
	require.NoError(t, s.Apply(createCmd))

	// Join group
	joinCmd := makeCommand(t, raft.CmdJoinGroup, struct {
		GroupID   string   `msgpack:"group_id"`
		MemberID  string   `msgpack:"member_id"`
		Topics    []string `msgpack:"topics"`
		SessionMs int32    `msgpack:"session_ms"`
	}{GroupID: "g1", MemberID: "m1", Topics: []string{"t1"}, SessionMs: 10000})
	require.NoError(t, s.Apply(joinCmd))

	s.mu.RLock()
	defer s.mu.RUnlock()

	group, ok := s.Groups["g1"]
	require.True(t, ok, "group should exist")
	assert.Equal(t, 1, len(group.Members), "should have 1 member")
	assert.Contains(t, group.Members, "m1", "member m1 should be in group")
}

// TestClusterState_JoinGroup_TwoMembers_BalancedAssignment verifies that
// 4 partitions are evenly divided between 2 members (2 each).
func TestClusterState_JoinGroup_TwoMembers_BalancedAssignment(t *testing.T) {
	s := NewClusterState()
	require.NotNil(t, s)

	registerBroker(t, s, "b1", "addr1")
	registerBroker(t, s, "b2", "addr2")

	createCmd := makeCommand(t, raft.CmdCreateTopic, struct {
		Topic      string `msgpack:"topic"`
		Partitions int32  `msgpack:"partitions"`
	}{Topic: "t1", Partitions: 4})
	require.NoError(t, s.Apply(createCmd))

	// Two members join
	for _, mid := range []string{"m1", "m2"} {
		joinCmd := makeCommand(t, raft.CmdJoinGroup, struct {
			GroupID   string   `msgpack:"group_id"`
			MemberID  string   `msgpack:"member_id"`
			Topics    []string `msgpack:"topics"`
			SessionMs int32    `msgpack:"session_ms"`
		}{GroupID: "g1", MemberID: mid, Topics: []string{"t1"}, SessionMs: 10000})
		require.NoError(t, s.Apply(joinCmd))
	}

	// Verify assignment via computeAssignment
	assignment := s.computeAssignment("g1")
	require.NotNil(t, assignment, "assignment should not be nil")

	// Total partitions assigned should be 4
	totalAssigned := 0
	for _, topicParts := range assignment {
		for _, parts := range topicParts {
			totalAssigned += len(parts)
		}
	}
	assert.Equal(t, 4, totalAssigned, "all 4 partitions should be assigned")
}

// TestClusterState_JoinGroup_MorePartitionsThanMembers verifies balanced
// assignment when partitions don't divide evenly among members.
func TestClusterState_JoinGroup_MorePartitionsThanMembers(t *testing.T) {
	s := NewClusterState()
	require.NotNil(t, s)

	registerBroker(t, s, "b1", "addr1")

	createCmd := makeCommand(t, raft.CmdCreateTopic, struct {
		Topic      string `msgpack:"topic"`
		Partitions int32  `msgpack:"partitions"`
	}{Topic: "t1", Partitions: 7})
	require.NoError(t, s.Apply(createCmd))

	// Three members join
	for _, mid := range []string{"m1", "m2", "m3"} {
		joinCmd := makeCommand(t, raft.CmdJoinGroup, struct {
			GroupID   string   `msgpack:"group_id"`
			MemberID  string   `msgpack:"member_id"`
			Topics    []string `msgpack:"topics"`
			SessionMs int32    `msgpack:"session_ms"`
		}{GroupID: "g1", MemberID: mid, Topics: []string{"t1"}, SessionMs: 10000})
		require.NoError(t, s.Apply(joinCmd))
	}

	assignment := s.computeAssignment("g1")
	require.NotNil(t, assignment)

	// Count per member
	memberCounts := make(map[string]int)
	for memberID, topicParts := range assignment {
		for _, parts := range topicParts {
			memberCounts[memberID] += len(parts)
		}
	}

	// 7 partitions / 3 members should be ~(3,2,2)
	totalAssigned := 0
	for _, count := range memberCounts {
		totalAssigned += count
		assert.GreaterOrEqual(t, count, 2, "each member should have at least 2 partitions")
		assert.LessOrEqual(t, count, 3, "no member should have more than 3 partitions")
	}
	assert.Equal(t, 7, totalAssigned, "all 7 partitions should be assigned")
}

// TestClusterState_MetadataVersion_Increments verifies that each mutation
// (broker registration, topic creation) bumps MetadataVersion.
func TestClusterState_MetadataVersion_Increments(t *testing.T) {
	s := NewClusterState()
	require.NotNil(t, s)

	s.mu.RLock()
	v0 := s.MetadataVersion
	s.mu.RUnlock()

	registerBroker(t, s, "b1", "addr1")
	s.mu.RLock()
	v1 := s.MetadataVersion
	s.mu.RUnlock()
	assert.Greater(t, v1, v0, "version should increase after broker registration")

	createCmd := makeCommand(t, raft.CmdCreateTopic, struct {
		Topic      string `msgpack:"topic"`
		Partitions int32  `msgpack:"partitions"`
	}{Topic: "t1", Partitions: 3})
	require.NoError(t, s.Apply(createCmd))

	s.mu.RLock()
	v2 := s.MetadataVersion
	s.mu.RUnlock()
	assert.Greater(t, v2, v1, "version should increase after topic creation")
}
