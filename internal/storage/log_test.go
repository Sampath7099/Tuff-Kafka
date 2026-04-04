package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPartitionLog_AppendAndRead verifies that a single record can be appended
// and then read back with matching key and value.
func TestPartitionLog_AppendAndRead(t *testing.T) {
	dir := t.TempDir()
	log, err := NewPartitionLog(dir, "orders", 0)
	require.NoError(t, err, "NewPartitionLog should succeed")
	defer log.Close()

	offset, err := log.Append("user-42", []byte("order-data"))
	require.NoError(t, err, "Append should succeed")
	assert.Equal(t, int64(0), offset, "first offset should be 0")

	key, value, err := log.Read(0)
	require.NoError(t, err, "Read should succeed")
	assert.Equal(t, "user-42", key, "key should match")
	assert.Equal(t, []byte("order-data"), value, "value should match")
}

// TestPartitionLog_MultipleAppends writes 1000 records and reads them all back,
// verifying monotonically increasing offsets and correct data.
func TestPartitionLog_MultipleAppends(t *testing.T) {
	dir := t.TempDir()
	log, err := NewPartitionLog(dir, "events", 0)
	require.NoError(t, err)
	defer log.Close()

	const N = 1000
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		offset, err := log.Append(key, value)
		require.NoError(t, err, "Append %d should succeed", i)
		assert.Equal(t, int64(i), offset, "offset should be sequential")
	}

	assert.Equal(t, int64(N), log.NextOffset(), "NextOffset should be N after N appends")

	// Read all back and verify
	for i := 0; i < N; i++ {
		key, value, err := log.Read(int64(i))
		require.NoError(t, err, "Read offset %d should succeed", i)
		assert.Equal(t, fmt.Sprintf("key-%d", i), key, "key mismatch at offset %d", i)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value, "value mismatch at offset %d", i)
	}
}

// TestPartitionLog_ReadBatch_RespectsByteLimit verifies that ReadBatch stops
// returning records when the cumulative byte count exceeds maxBytes.
// At least one record is always returned.
func TestPartitionLog_ReadBatch_RespectsByteLimit(t *testing.T) {
	dir := t.TempDir()
	log, err := NewPartitionLog(dir, "batch-test", 0)
	require.NoError(t, err)
	defer log.Close()

	// Write 10 records, each ~20 bytes (key=5 + value=15)
	for i := 0; i < 10; i++ {
		_, err := log.Append(fmt.Sprintf("k%04d", i), []byte("0123456789abcde"))
		require.NoError(t, err)
	}

	// maxBytes = 50 should return ~2-3 records (each is ~20 bytes)
	records, err := log.ReadBatch(0, 50)
	require.NoError(t, err, "ReadBatch should succeed")
	assert.GreaterOrEqual(t, len(records), 1, "at least one record should be returned")
	assert.LessOrEqual(t, len(records), 4, "should not return too many records")

	// Verify records are in order starting from offset 0
	for i, r := range records {
		assert.Equal(t, int64(i), r.Offset, "record offsets should be sequential")
	}
}

// TestPartitionLog_ReadBatch_ReturnsAllWhenUnderLimit verifies that when
// maxBytes is large enough, all records from the starting offset are returned.
func TestPartitionLog_ReadBatch_ReturnsAllWhenUnderLimit(t *testing.T) {
	dir := t.TempDir()
	log, err := NewPartitionLog(dir, "batch-all", 0)
	require.NoError(t, err)
	defer log.Close()

	for i := 0; i < 5; i++ {
		_, err := log.Append("k", []byte("v"))
		require.NoError(t, err)
	}

	// maxBytes very large — should return all 5 records
	records, err := log.ReadBatch(0, 1024*1024)
	require.NoError(t, err)
	assert.Equal(t, 5, len(records), "should return all 5 records")
}

// TestPartitionLog_RebuildIndexOnReopen closes the log and reopens it,
// verifying that the index is correctly rebuilt from the file and all
// previously written records are still readable.
func TestPartitionLog_RebuildIndexOnReopen(t *testing.T) {
	dir := t.TempDir()

	// Write some records
	func() {
		log, err := NewPartitionLog(dir, "persist", 0)
		require.NoError(t, err)
		defer log.Close()

		_, err = log.Append("k1", []byte("v1"))
		require.NoError(t, err)
		_, err = log.Append("k2", []byte("v2"))
		require.NoError(t, err)
		_, err = log.Append("k3", []byte("v3"))
		require.NoError(t, err)
	}()

	// Reopen
	log, err := NewPartitionLog(dir, "persist", 0)
	require.NoError(t, err)
	defer log.Close()

	assert.Equal(t, int64(3), log.NextOffset(), "NextOffset should be 3 after reopen")

	key, value, err := log.Read(0)
	require.NoError(t, err)
	assert.Equal(t, "k1", key)
	assert.Equal(t, []byte("v1"), value)

	key, value, err = log.Read(2)
	require.NoError(t, err)
	assert.Equal(t, "k3", key)
	assert.Equal(t, []byte("v3"), value)

	// Should be able to append after reopen
	offset, err := log.Append("k4", []byte("v4"))
	require.NoError(t, err)
	assert.Equal(t, int64(3), offset, "new append should get offset 3")
}

// TestPartitionLog_ReadOutOfRange_Negative verifies that Read returns an
// error when given a negative offset.
func TestPartitionLog_ReadOutOfRange_Negative(t *testing.T) {
	dir := t.TempDir()
	log, err := NewPartitionLog(dir, "range", 0)
	require.NoError(t, err)
	defer log.Close()

	_, _, err = log.Read(-1)
	require.Error(t, err, "Read with negative offset should fail")
	assert.Contains(t, err.Error(), "out of range", "error should mention out of range")
}

// TestPartitionLog_ReadOutOfRange_PastEnd verifies that Read returns an
// error when given an offset beyond the current log end.
func TestPartitionLog_ReadOutOfRange_PastEnd(t *testing.T) {
	dir := t.TempDir()
	log, err := NewPartitionLog(dir, "range", 0)
	require.NoError(t, err)
	defer log.Close()

	_, err = log.Append("k", []byte("v"))
	require.NoError(t, err)

	_, _, err = log.Read(1) // only offset 0 exists
	require.Error(t, err, "Read beyond end should fail")

	_, _, err = log.Read(100)
	require.Error(t, err, "Read far beyond end should fail")
}

// TestPartitionLog_EmptyKey verifies that records with an empty key
// are correctly stored and read back.
func TestPartitionLog_EmptyKey(t *testing.T) {
	dir := t.TempDir()
	log, err := NewPartitionLog(dir, "empty-key", 0)
	require.NoError(t, err)
	defer log.Close()

	offset, err := log.Append("", []byte("value-without-key"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), offset)

	key, value, err := log.Read(0)
	require.NoError(t, err)
	assert.Equal(t, "", key, "key should be empty string")
	assert.Equal(t, []byte("value-without-key"), value)
}

// TestPartitionLog_ConcurrentAppends verifies that multiple goroutines can
// safely append to the same log concurrently without data races or
// duplicate/missing offsets.
func TestPartitionLog_ConcurrentAppends(t *testing.T) {
	dir := t.TempDir()
	log, err := NewPartitionLog(dir, "concurrent", 0)
	require.NoError(t, err)
	defer log.Close()

	const numGoroutines = 50
	const appendsPerGoroutine = 20

	var wg sync.WaitGroup
	offsets := make(chan int64, numGoroutines*appendsPerGoroutine)

	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < appendsPerGoroutine; i++ {
				key := fmt.Sprintf("g%d-i%d", gid, i)
				value := []byte(fmt.Sprintf("data-%d-%d", gid, i))
				off, err := log.Append(key, value)
				assert.NoError(t, err)
				offsets <- off
			}
		}(g)
	}
	wg.Wait()
	close(offsets)

	// Collect all offsets and verify uniqueness
	seen := make(map[int64]bool)
	for off := range offsets {
		assert.False(t, seen[off], "offset %d should be unique", off)
		seen[off] = true
	}

	totalExpected := numGoroutines * appendsPerGoroutine
	assert.Equal(t, totalExpected, len(seen), "should have exactly %d unique offsets", totalExpected)
	assert.Equal(t, int64(totalExpected), log.NextOffset(), "NextOffset should match total appends")

	// Verify all records are readable
	for off := int64(0); off < int64(totalExpected); off++ {
		key, value, err := log.Read(off)
		assert.NoError(t, err, "Read offset %d should succeed", off)
		assert.NotEmpty(t, key, "key at offset %d should not be empty", off)
		assert.NotEmpty(t, value, "value at offset %d should not be empty", off)
	}
}
