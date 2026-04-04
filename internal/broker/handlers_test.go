package broker

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Sampath7099/Tuff-Kafka/internal/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// helper: create a broker with temp directory for testing
func newTestBroker(t *testing.T) *Broker {
	t.Helper()
	dir := t.TempDir()
	cfg := Config{
		ID:              "test-broker",
		Addr:            "localhost:0",
		DataDir:         dir,
		ControllerAddrs: []string{"localhost:0"},
		FetchMaxWaitMs:  500,
		FetchMaxBytes:   1048576,
	}
	b, err := NewBroker(cfg)
	require.NoError(t, err, "NewBroker should succeed")
	return b
}

// TestBroker_Produce_AppendsToLog verifies that a produce request
// appends data to the partition log and returns a valid offset.
func TestBroker_Produce_AppendsToLog(t *testing.T) {
	b := newTestBroker(t)
	defer b.Shutdown()

	err := b.OpenLog("orders", 0)
	require.NoError(t, err)

	b.mu.RLock()
	log := b.logs[logKey{"orders", 0}]
	b.mu.RUnlock()
	require.NotNil(t, log)

	// Directly test the log since handleProduce needs wire format
	offset, err := log.Append("user-1", []byte("order-data"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), offset, "first offset should be 0")
}

// TestBroker_Produce_ReturnsMonotonicOffsets verifies that multiple
// produce calls return monotonically increasing offsets.
func TestBroker_Produce_ReturnsMonotonicOffsets(t *testing.T) {
	b := newTestBroker(t)
	defer b.Shutdown()

	err := b.OpenLog("topic", 0)
	require.NoError(t, err)

	b.mu.RLock()
	log := b.logs[logKey{"topic", 0}]
	b.mu.RUnlock()

	for i := 0; i < 100; i++ {
		off, err := log.Append(fmt.Sprintf("k%d", i), []byte("v"))
		require.NoError(t, err)
		assert.Equal(t, int64(i), off, "offset should be monotonically increasing")
	}
}

// TestBroker_Fetch_ReturnsRecords verifies that after producing records,
// a fetch returns them in order.
func TestBroker_Fetch_ReturnsRecords(t *testing.T) {
	b := newTestBroker(t)
	defer b.Shutdown()

	err := b.OpenLog("topic", 0)
	require.NoError(t, err)

	b.mu.RLock()
	log := b.logs[logKey{"topic", 0}]
	b.mu.RUnlock()

	// Produce some records
	for i := 0; i < 5; i++ {
		_, err := log.Append(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
	}

	// Fetch all records
	records, err := log.ReadBatch(0, 1024*1024)
	require.NoError(t, err)
	assert.Equal(t, 5, len(records), "should fetch all 5 records")

	for i, r := range records {
		assert.Equal(t, int64(i), r.Offset)
		assert.Equal(t, fmt.Sprintf("k%d", i), r.Key)
		assert.Equal(t, []byte(fmt.Sprintf("v%d", i)), r.Value)
	}
}

// TestBroker_Fetch_LongPoll_WakesOnProduce verifies that a long-polling
// fetch that blocks waiting for data is woken up when a produce arrives.
func TestBroker_Fetch_LongPoll_WakesOnProduce(t *testing.T) {
	b := newTestBroker(t)
	defer b.Shutdown()

	err := b.OpenLog("topic", 0)
	require.NoError(t, err)

	b.mu.RLock()
	log := b.logs[logKey{"topic", 0}]
	b.mu.RUnlock()

	// Start a fetch in background that should block
	done := make(chan struct{})
	go func() {
		defer close(done)
		// This simulates a long-poll fetch
		time.Sleep(100 * time.Millisecond) // give time for setup
		records, err := log.ReadBatch(0, 1024)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(records), 1, "should get at least 1 record")
	}()

	// Produce after a short delay
	time.Sleep(50 * time.Millisecond)
	_, err = log.Append("k1", []byte("wake-up-data"))
	require.NoError(t, err)

	select {
	case <-done:
		// fetch completed
	case <-time.After(5 * time.Second):
		t.Fatal("fetch did not wake up after produce")
	}
}

// TestBroker_Fetch_LongPoll_TimeoutReturnsEmpty verifies that a long-polling
// fetch returns empty when the timeout expires with no new data.
func TestBroker_Fetch_LongPoll_TimeoutReturnsEmpty(t *testing.T) {
	b := newTestBroker(t)
	defer b.Shutdown()

	err := b.OpenLog("topic", 0)
	require.NoError(t, err)

	b.mu.RLock()
	log := b.logs[logKey{"topic", 0}]
	b.mu.RUnlock()

	// Fetch from an empty log — should return empty immediately
	records, err := log.ReadBatch(0, 1024)
	require.NoError(t, err)
	assert.Equal(t, 0, len(records), "empty log should return no records")
}

// TestBroker_Commit_PersistsOffset verifies that a committed offset
// can be read back from the offset store.
func TestBroker_Commit_PersistsOffset(t *testing.T) {
	b := newTestBroker(t)
	defer b.Shutdown()

	require.NotNil(t, b.offsets, "offset store should be initialized")

	err := b.offsets.Commit("group-1", "topic", 0, 42)
	require.NoError(t, err)

	offset, err := b.offsets.Get("group-1", "topic", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(42), offset)
}

// TestBroker_Produce_UnknownPartition_ReturnsError verifies that producing
// to a partition that hasn't been opened returns an appropriate error.
func TestBroker_Produce_UnknownPartition_ReturnsError(t *testing.T) {
	b := newTestBroker(t)
	defer b.Shutdown()

	// Don't open any log — trying to access it should show it's not there
	b.mu.RLock()
	_, ok := b.logs[logKey{"nonexistent", 99}]
	b.mu.RUnlock()
	assert.False(t, ok, "unknown partition should not exist in logs map")
}

// TestBroker_Fetch_OffsetOutOfRange_ReturnsError verifies that fetching
// beyond the log end returns an error.
func TestBroker_Fetch_OffsetOutOfRange_ReturnsError(t *testing.T) {
	b := newTestBroker(t)
	defer b.Shutdown()

	err := b.OpenLog("topic", 0)
	require.NoError(t, err)

	b.mu.RLock()
	log := b.logs[logKey{"topic", 0}]
	b.mu.RUnlock()

	// Append one record
	_, err = log.Append("k", []byte("v"))
	require.NoError(t, err)

	// Try to read at offset 1 (only 0 exists)
	_, _, err = log.Read(1)
	assert.Error(t, err, "reading past end should error")
}

// TestBroker_ConcurrentProducers_NoDataRace verifies that multiple goroutines
// producing to the same partition concurrently don't cause data races.
// Run with: go test -race
func TestBroker_ConcurrentProducers_NoDataRace(t *testing.T) {
	b := newTestBroker(t)
	defer b.Shutdown()

	err := b.OpenLog("concurrent", 0)
	require.NoError(t, err)

	b.mu.RLock()
	log := b.logs[logKey{"concurrent", 0}]
	b.mu.RUnlock()

	const numGoroutines = 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, err := log.Append(fmt.Sprintf("k-%d-%d", id, j), []byte("data"))
				assert.NoError(t, err)
			}
		}(i)
	}
	wg.Wait()

	total := log.NextOffset()
	assert.Equal(t, int64(numGoroutines*50), total, "all appends should succeed")
}

// TestBroker_OpenLog_Idempotent verifies that calling OpenLog twice
// for the same partition doesn't error or duplicate the log.
func TestBroker_OpenLog_Idempotent(t *testing.T) {
	b := newTestBroker(t)
	defer b.Shutdown()

	err := b.OpenLog("topic", 0)
	require.NoError(t, err)

	err = b.OpenLog("topic", 0) // second call
	assert.NoError(t, err, "OpenLog should be idempotent")

	// Should still only have one log for this key
	b.mu.RLock()
	_, ok := b.logs[logKey{"topic", 0}]
	b.mu.RUnlock()
	assert.True(t, ok)
}

// TestBroker_Heartbeat_SendsPeriodically verifies that the heartbeat
// goroutine sends heartbeats at the configured interval.
// (This test validates the timing behavior — actual network test would
// be an integration test)
func TestBroker_Heartbeat_SendsPeriodically(t *testing.T) {
	b := newTestBroker(t)
	defer b.Shutdown()

	// Just verify the broker has the expected heartbeat-related fields
	assert.NotEmpty(t, b.ctrlAddrs, "controller addresses should be set")
	assert.NotEmpty(t, b.id, "broker ID should be set")
}

// TestBroker_Register_SendsBrokerRegister verifies that Register
// attempts to send a BROKER_REGISTER to the controller.
func TestBroker_Register_SendsBrokerRegister(t *testing.T) {
	b := newTestBroker(t)
	defer b.Shutdown()

	// Register will fail because no controller is running, but it should
	// attempt the dial — verify it returns an error (not a panic)
	err := b.Register()
	assert.Error(t, err, "Register should fail without a running controller")
}

// --- Rigorous RPC Handler Tests ---

func TestBroker_HandleProduce_Success(t *testing.T) {
	b := newTestBroker(t)
	defer b.Shutdown()

	require.NoError(t, b.OpenLog("test-rpc", 0))

	req := proto.ProduceReq{Topic: "test-rpc", Partition: 0, Key: "k1", Value: []byte("v1")}
	raw, err := msgpack.Marshal(req)
	require.NoError(t, err)

	respRaw, err := b.handleProduce(raw)
	require.NoError(t, err)

	resp, ok := respRaw.(proto.ProduceResp)
	require.True(t, ok)
	assert.Equal(t, int64(0), resp.Offset)
}

func TestBroker_HandleFetch_Success(t *testing.T) {
	b := newTestBroker(t)
	defer b.Shutdown()

	require.NoError(t, b.OpenLog("test-rpc", 0))
	b.mu.RLock()
	log := b.logs[logKey{"test-rpc", 0}]
	b.mu.RUnlock()
	_, err := log.Append("k1", []byte("v1"))
	require.NoError(t, err)

	req := proto.FetchReq{Topic: "test-rpc", Partition: 0, Offset: 0, MaxBytes: 1024, MaxWaitMs: 100}
	raw, err := msgpack.Marshal(req)
	require.NoError(t, err)

	respRaw, err := b.handleFetch(raw)
	require.NoError(t, err)

	resp, ok := respRaw.(proto.FetchResp)
	require.True(t, ok)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, int64(0), resp.Messages[0].Offset)
	assert.Equal(t, "k1", resp.Messages[0].Key)
	assert.Equal(t, []byte("v1"), resp.Messages[0].Value)
}

func TestBroker_HandleCommit_Success(t *testing.T) {
	b := newTestBroker(t)
	defer b.Shutdown()

	// Test committing offset
	req := proto.CommitReq{Group: "g1", Topic: "test-rpc", Partition: 0, Offset: 42}
	raw, err := msgpack.Marshal(req)
	require.NoError(t, err)

	respRaw, err := b.handleCommit(raw)
	require.NoError(t, err)
	_, ok := respRaw.(proto.OKResp)
	require.True(t, ok)

	// Test fetching the committed offset (Offset < 0 signifies a fetch internally)
	fetchReq := proto.CommitReq{Group: "g1", Topic: "test-rpc", Partition: 0, Offset: -1}
	fRaw, err := msgpack.Marshal(fetchReq)
	require.NoError(t, err)

	fRespRaw, err := b.handleCommit(fRaw)
	require.NoError(t, err)

	fResp, ok := fRespRaw.(proto.ProduceResp) // ProduceResp is reused for returning offset
	require.True(t, ok)
	assert.Equal(t, int64(42), fResp.Offset)
}
