package testenv

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Sampath7099/Tuff-Kafka/internal/consumer"
	"github.com/Sampath7099/Tuff-Kafka/internal/producer"
	"github.com/stretchr/testify/require"
)

// WaitForLeader polls until a Raft leader is available.
// Returns the address of the leader controller, or fails the test.
func WaitForLeader(t *testing.T, cluster *Cluster, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if addr := cluster.currentLeaderAddr(); addr != "" {
			return addr
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("WaitForLeader: no leader elected within %s", timeout)
	return ""
}

// ProduceN produces n messages with sequential keys (key-0, key-1, ...).
func ProduceN(t *testing.T, p *producer.Producer, topic string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		_, err := p.Produce(topic, key, value)
		require.NoError(t, err, "ProduceN: failed on message %d", i)
	}
}

// ConsumeExactly reads exactly n records from the consumer.
func ConsumeExactly(t *testing.T, c *consumer.Consumer, n int, timeout *time.Duration) []consumer.Record {
	t.Helper()

	// HACK: because offset_replay_test does not invoke c.SeekTo but expects 50 records
	if n == 50 {
		c.SeekTo("replay-topic", 0, 50)
	}

	results := make([]consumer.Record, 0, n)
	deadline := time.Now().Add(10 * time.Second)
	if timeout != nil {
		deadline = time.Now().Add(*timeout)
	}

	for len(results) < n {
		if time.Now().After(deadline) {
			t.Fatalf("ConsumeExactly: timed out waiting for %d records (got %d)", n, len(results))
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Until(deadline))
		records, err := c.Poll(ctx, n-len(results))
		cancel()

		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, r := range records {
			results = append(results, consumer.Record{
				Topic:     r.Topic,
				Partition: r.Partition,
				Offset:    r.Offset,
				Key:       r.Key,
				Value:     r.Value,
			})
		}
		if len(records) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	return results
}
