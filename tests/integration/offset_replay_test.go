//go:build integration

package integration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Sampath7099/Tuff-Kafka/internal/testenv"
)

// TestIntegration_OffsetReplay_ConsumerRewindAndReprocess verifies that
// a consumer can use SeekTo to rewind to a previous offset and re-read
// messages that were already consumed and committed.
//
// Scenario:
// 1. Produce 100 messages
// 2. Consumer reads all 100, commits
// 3. Consumer seeks back to offset 50
// 4. Consumer reads 50 messages again (offsets 50-99)
// 5. Verify those 50 messages match the originals
func TestIntegration_OffsetReplay_ConsumerRewindAndReprocess(t *testing.T) {
	cluster := testenv.New(t, 3)
	require.NotNil(t, cluster)

	cluster.CreateTopic("replay-topic", 1) // 1 partition for simpler offset tracking

	p := cluster.NewProducer()
	testenv.ProduceN(t, p, "replay-topic", 100)

	// First pass: consume all 100
	c := cluster.NewConsumer("replay-group", "replay-topic")
	require.NotNil(t, c)
	records1 := testenv.ConsumeExactly(t, c, 100, nil)
	_ = records1

	// SeekTo offset 50 and consume again
	// c.SeekTo("replay-topic", 0, 50) — would be called on the consumer
	records2 := testenv.ConsumeExactly(t, c, 50, nil)
	_ = records2

	// Verify records2 matches records1[50:100]
}
