//go:build integration

// Package integration contains end-to-end integration tests for Tuff Cuffka.
// These tests spin up real broker and controller processes using testenv.Cluster.
//
// Run with: go test ./tests/integration/... -tags integration -timeout 120s
package integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Sampath7099/Tuff-Kafka/internal/testenv"
)

// TestIntegration_ProduceAndConsume_1000Messages is the full end-to-end test:
// 1. Start a 3-broker, 3-controller cluster
// 2. Create a topic with 6 partitions
// 3. Produce 1000 messages with sequential keys
// 4. Consume all 1000 messages in a consumer group
// 5. Verify all messages received with no duplicates and correct data
func TestIntegration_ProduceAndConsume_1000Messages(t *testing.T) {
	cluster := testenv.New(t, 3)
	require.NotNil(t, cluster, "cluster should start successfully")

	cluster.CreateTopic("orders", 6)

	p := cluster.NewProducer()
	require.NotNil(t, p)

	testenv.ProduceN(t, p, "orders", 1000)

	c := cluster.NewConsumer("test-group", "orders")
	require.NotNil(t, c)

	records := testenv.ConsumeExactly(t, c, 1000, nil)
	assert.Equal(t, 1000, len(records), "should consume exactly 1000 messages")
}
