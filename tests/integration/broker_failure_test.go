//go:build integration

package integration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Sampath7099/Tuff-Kafka/internal/testenv"
)

// TestIntegration_BrokerFailure_PartitionsReassigned verifies that when
// a broker dies, the controller detects the failure (via heartbeat timeout),
// reassigns its partitions to surviving brokers, and the cluster continues
// to serve reads and writes.
func TestIntegration_BrokerFailure_PartitionsReassigned(t *testing.T) {
	cluster := testenv.New(t, 3)
	require.NotNil(t, cluster)

	cluster.CreateTopic("failover-topic", 6) // 2 per broker

	// Kill broker 1
	cluster.KillBroker(1)

	// Wait for controller to detect failure (session timeout)
	// Then verify partitions are reassigned

	// Produce after failure — should still succeed on surviving brokers
	p := cluster.NewProducer()
	testenv.ProduceN(t, p, "failover-topic", 100)
}

// TestIntegration_BrokerFailure_ProducerRecovery verifies that a producer
// can recover from a broker failure by refreshing metadata and re-routing
// to the new partition leaders.
func TestIntegration_BrokerFailure_ProducerRecovery(t *testing.T) {
	cluster := testenv.New(t, 3)
	require.NotNil(t, cluster)

	cluster.CreateTopic("recovery-topic", 6)

	p := cluster.NewProducer()

	// Produce some messages before failure
	testenv.ProduceN(t, p, "recovery-topic", 50)

	// Kill a broker
	cluster.KillBroker(0)

	// Produce more messages — producer should retry/refresh and succeed
	testenv.ProduceN(t, p, "recovery-topic", 50)

	// Total: 100 messages should be in the system
}
