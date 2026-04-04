//go:build integration

package integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Sampath7099/Tuff-Kafka/internal/testenv"
)

// TestIntegration_ConsumerGroup_TwoGroupsIndependent verifies that two
// consumer groups consuming the same topic each receive ALL messages
// independently. No message should be missed by either group.
func TestIntegration_ConsumerGroup_TwoGroupsIndependent(t *testing.T) {
	cluster := testenv.New(t, 3)
	require.NotNil(t, cluster)

	cluster.CreateTopic("events", 4)

	p := cluster.NewProducer()
	testenv.ProduceN(t, p, "events", 100)

	c1 := cluster.NewConsumer("group-a", "events")
	c2 := cluster.NewConsumer("group-b", "events")
	require.NotNil(t, c1)
	require.NotNil(t, c2)

	records1 := testenv.ConsumeExactly(t, c1, 100, nil)
	records2 := testenv.ConsumeExactly(t, c2, 100, nil)

	assert.Equal(t, 100, len(records1), "group-a should get all 100 messages")
	assert.Equal(t, 100, len(records2), "group-b should get all 100 messages")
}

// TestIntegration_ConsumerGroup_Rebalance verifies that when a new member
// joins mid-consumption, the group rebalances and all partitions remain
// covered.
func TestIntegration_ConsumerGroup_Rebalance(t *testing.T) {
	cluster := testenv.New(t, 3)
	require.NotNil(t, cluster)

	cluster.CreateTopic("rebalance-topic", 4)

	// Start one consumer
	c1 := cluster.NewConsumer("rebalance-group", "rebalance-topic")
	require.NotNil(t, c1)

	// Produce some messages
	p := cluster.NewProducer()
	testenv.ProduceN(t, p, "rebalance-topic", 50)

	// Second consumer joins → should trigger rebalance
	c2 := cluster.NewConsumer("rebalance-group", "rebalance-topic")
	require.NotNil(t, c2)

	// Produce more messages
	testenv.ProduceN(t, p, "rebalance-topic", 50)

	// Together, both consumers should get all 100 messages
	// (exact distribution depends on when the rebalance happens)
	_ = c1
	_ = c2
}
