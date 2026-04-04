//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Sampath7099/Tuff-Kafka/internal/testenv"
)

// TestIntegration_ControllerFailover_ElectsNewLeader verifies that when
// the Raft leader controller is killed, the remaining two controllers
// elect a new leader within the election timeout.
func TestIntegration_ControllerFailover_ElectsNewLeader(t *testing.T) {
	cluster := testenv.New(t, 3)
	require.NotNil(t, cluster)

	// Wait for initial leader
	leader1 := testenv.WaitForLeader(t, cluster, 5*time.Second)
	require.NotEmpty(t, leader1, "initial leader should be elected")

	// Kill the leader
	cluster.KillControllerLeader()

	// Wait for new leader
	leader2 := testenv.WaitForLeader(t, cluster, 5*time.Second)
	require.NotEmpty(t, leader2, "new leader should be elected after failover")

	// New leader should be different from the killed one
	assert.NotEqual(t, leader1, leader2, "new leader should be a different node")
}

// TestIntegration_ControllerFailover_MetadataIntact verifies that after
// a controller failover, all previously created topic metadata is intact
// (replicated via Raft log).
func TestIntegration_ControllerFailover_MetadataIntact(t *testing.T) {
	cluster := testenv.New(t, 3)
	require.NotNil(t, cluster)

	// Create topics before failover
	cluster.CreateTopic("topic-a", 3)
	cluster.CreateTopic("topic-b", 6)

	// Kill leader
	cluster.KillControllerLeader()

	// Wait for new leader
	testenv.WaitForLeader(t, cluster, 5*time.Second)

	// Verify metadata is still intact by creating a producer
	// (which fetches METADATA) and producing successfully
	p := cluster.NewProducer()
	require.NotNil(t, p)
	testenv.ProduceN(t, p, "topic-a", 10)
	testenv.ProduceN(t, p, "topic-b", 10)
}
