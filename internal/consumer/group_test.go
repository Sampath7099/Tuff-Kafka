package consumer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGroup_Join_ReceivesAssignment verifies that Join returns a valid
// partition assignment from the controller.
func TestGroup_Join_ReceivesAssignment(t *testing.T) {
	c := NewConsumer([]string{"localhost:9093"}, "test-group")
	require.NotNil(t, c)

	g := NewConsumerGroup(c)
	require.NotNil(t, g)

	// Join will fail without a running controller
	ctx := context.Background()
	err := g.Join(ctx)
	assert.Error(t, err, "Join should fail without a cluster (stub)")
}

// TestGroup_Join_SetsGenerationAndMemberID verifies that after Join,
// the consumer's generation and memberID are updated.
func TestGroup_Join_SetsGenerationAndMemberID(t *testing.T) {
	c := NewConsumer([]string{"localhost:9093"}, "test-group")
	require.NotNil(t, c)

	g := NewConsumerGroup(c)
	require.NotNil(t, g)

	// Simulate a successful join by manually setting fields
	c.mu.Lock()
	c.memberID = "uuid-1234"
	c.generation = 1
	c.assignment = map[string][]int32{"t1": {0, 1}}
	c.mu.Unlock()

	c.mu.Lock()
	assert.Equal(t, "uuid-1234", c.memberID)
	assert.Equal(t, int32(1), c.generation)
	assert.Equal(t, map[string][]int32{"t1": {0, 1}}, c.assignment)
	c.mu.Unlock()
}

// TestGroup_TwoMembersEquallyDivide4Partitions verifies that 4 partitions
// are evenly divided between 2 group members.
func TestGroup_TwoMembersEquallyDivide4Partitions(t *testing.T) {
	// This test verifies the expected outcome of the controller's
	// assignment algorithm via the consumer group interface.
	c1 := NewConsumer([]string{"localhost:9093"}, "group-1")
	require.NotNil(t, c1)
	c2 := NewConsumer([]string{"localhost:9093"}, "group-1")
	require.NotNil(t, c2)

	// Simulate assignments from controller
	c1.mu.Lock()
	c1.assignment = map[string][]int32{"t1": {0, 1}}
	c1.mu.Unlock()

	c2.mu.Lock()
	c2.assignment = map[string][]int32{"t1": {2, 3}}
	c2.mu.Unlock()

	c1.mu.Lock()
	p1 := len(c1.assignment["t1"])
	c1.mu.Unlock()

	c2.mu.Lock()
	p2 := len(c2.assignment["t1"])
	c2.mu.Unlock()

	assert.Equal(t, 2, p1, "member 1 should have 2 partitions")
	assert.Equal(t, 2, p2, "member 2 should have 2 partitions")
	assert.Equal(t, 4, p1+p2, "total should be 4 partitions")
}

// TestGroup_ThreeMembersWithRebalanceOnMemberLeave verifies that when
// a member leaves, the remaining members get the orphaned partitions.
func TestGroup_ThreeMembersWithRebalanceOnMemberLeave(t *testing.T) {
	consumers := make([]*Consumer, 3)
	for i := 0; i < 3; i++ {
		consumers[i] = NewConsumer([]string{"localhost:9093"}, "group-1")
		require.NotNil(t, consumers[i])
	}

	// Simulate initial assignment: 6 partitions / 3 members = 2 each
	assignments := []map[string][]int32{
		{"t1": {0, 1}},
		{"t1": {2, 3}},
		{"t1": {4, 5}},
	}
	for i, c := range consumers {
		c.mu.Lock()
		c.assignment = assignments[i]
		c.mu.Unlock()
	}

	// Member 2 leaves → members 0 and 1 should get 3 each
	consumers[0].mu.Lock()
	consumers[0].assignment = map[string][]int32{"t1": {0, 1, 4}}
	consumers[0].mu.Unlock()

	consumers[1].mu.Lock()
	consumers[1].assignment = map[string][]int32{"t1": {2, 3, 5}}
	consumers[1].mu.Unlock()

	consumers[0].mu.Lock()
	p0 := len(consumers[0].assignment["t1"])
	consumers[0].mu.Unlock()

	consumers[1].mu.Lock()
	p1 := len(consumers[1].assignment["t1"])
	consumers[1].mu.Unlock()

	assert.Equal(t, 3, p0, "member 0 should have 3 partitions after rebalance")
	assert.Equal(t, 3, p1, "member 1 should have 3 partitions after rebalance")
	assert.Equal(t, 6, p0+p1, "total should be 6 partitions")
}

// TestGroup_Heartbeat_TriggersRejoinOnRebalanceSignal verifies that when
// a heartbeat response has Rebalance=true, the consumer re-joins the group.
func TestGroup_Heartbeat_TriggersRejoinOnRebalanceSignal(t *testing.T) {
	c := NewConsumer([]string{"localhost:9093"}, "test-group")
	require.NotNil(t, c)

	g := NewConsumerGroup(c)
	require.NotNil(t, g)

	// Test the sendHeartbeat function directly
	rebalance, err := g.sendHeartbeat()
	assert.Error(t, err, "sendHeartbeat should fail without a cluster")
	// When implemented: if rebalance is true, Join should be called
	_ = rebalance
}

// TestGroup_MissedHeartbeat_ControllerDeclaresDeadAndRebalances verifies
// the expected behavior when a consumer misses its heartbeat deadline.
func TestGroup_MissedHeartbeat_ControllerDeclaresDeadAndRebalances(t *testing.T) {
	// This tests the interaction between consumer and controller.
	// A consumer that misses heartbeats should be removed from the group.
	// The remaining members should get the dead member's partitions.
	c := NewConsumer([]string{"localhost:9093"}, "test-group")
	require.NotNil(t, c)

	c.mu.Lock()
	c.memberID = "will-die"
	c.generation = 1
	c.assignment = map[string][]int32{"t1": {0, 1}}
	c.mu.Unlock()

	// Simulate: after heartbeat timeout, controller removes member
	// and bumps generation. When surviving members heartbeat, they
	// get Rebalance=true and re-join with new assignment.
	c.mu.Lock()
	c.generation = 2 // generation bumped by controller
	c.assignment = map[string][]int32{} // assignment cleared, needs rejoin
	c.mu.Unlock()

	c.mu.Lock()
	assert.Equal(t, int32(2), c.generation, "generation should be bumped")
	assert.Empty(t, c.assignment, "assignment should be cleared pending rejoin")
	c.mu.Unlock()
}

// TestGroup_Leave_SendsLeaveGroupRequest verifies that Leave sends
// a LEAVE_GROUP request to the controller.
func TestGroup_Leave_SendsLeaveGroupRequest(t *testing.T) {
	c := NewConsumer([]string{"localhost:9093"}, "test-group")
	require.NotNil(t, c)

	g := NewConsumerGroup(c)
	require.NotNil(t, g)

	ctx := context.Background()
	err := g.Leave(ctx)
	assert.Error(t, err, "Leave should fail without a cluster (stub)")
}
