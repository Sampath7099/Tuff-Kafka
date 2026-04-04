package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOffsetStore_CommitAndGet verifies that committing an offset and then
// reading it back returns the committed value.
func TestOffsetStore_CommitAndGet(t *testing.T) {
	dir := t.TempDir()
	store, err := NewOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	err = store.Commit("group-1", "orders", 0, 42)
	require.NoError(t, err, "Commit should succeed")

	offset, err := store.Get("group-1", "orders", 0)
	require.NoError(t, err, "Get should succeed")
	assert.Equal(t, int64(42), offset, "offset should be 42")

	// Overwrite with a higher offset
	err = store.Commit("group-1", "orders", 0, 100)
	require.NoError(t, err)

	offset, err = store.Get("group-1", "orders", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(100), offset, "offset should be updated to 100")
}

// TestOffsetStore_ConcurrentCommits verifies that 50 goroutines committing
// to the same (group, topic, partition) do not cause data races or errors.
// The final committed value should be the value from the last commit that
// was processed.
func TestOffsetStore_ConcurrentCommits(t *testing.T) {
	dir := t.TempDir()
	store, err := NewOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	const numGoroutines = 50
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(offset int64) {
			defer wg.Done()
			err := store.Commit("concurrent-group", "topic", 0, offset)
			assert.NoError(t, err)
		}(int64(i))
	}
	wg.Wait()

	// The final value should be some valid offset (one of the committed values)
	offset, err := store.Get("concurrent-group", "topic", 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, offset, int64(0), "offset should be non-negative")
	assert.Less(t, offset, int64(numGoroutines), "offset should be from one of the commits")
}

// TestOffsetStore_GetBeforeAnyCommit verifies that Get returns -1 and no error
// when no offset has been committed for the given (group, topic, partition).
func TestOffsetStore_GetBeforeAnyCommit(t *testing.T) {
	dir := t.TempDir()
	store, err := NewOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	offset, err := store.Get("nonexistent-group", "nonexistent-topic", 0)
	require.NoError(t, err, "Get should not error for missing offset")
	assert.Equal(t, int64(-1), offset, "offset should be -1 when nothing committed")
}

// TestOffsetStore_PersistsAcrossRestart verifies that committed offsets
// survive store closure and reopening from the same directory.
func TestOffsetStore_PersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	// First store: commit an offset
	func() {
		store, err := NewOffsetStore(dir)
		require.NoError(t, err)
		defer store.Close()

		err = store.Commit("persist-group", "events", 3, 999)
		require.NoError(t, err)
	}()

	// Second store: reopen and read
	store, err := NewOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	offset, err := store.Get("persist-group", "events", 3)
	require.NoError(t, err)
	assert.Equal(t, int64(999), offset, "offset should persist across restart")
}

// TestOffsetStore_MultipleGroupsSamePartition verifies that two different
// consumer groups can independently track offsets for the same topic/partition
// without interfering with each other.
func TestOffsetStore_MultipleGroupsSamePartition(t *testing.T) {
	dir := t.TempDir()
	store, err := NewOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	// Group A commits offset 50
	err = store.Commit("group-a", "shared-topic", 0, 50)
	require.NoError(t, err)

	// Group B commits offset 100 to the SAME topic/partition
	err = store.Commit("group-b", "shared-topic", 0, 100)
	require.NoError(t, err)

	// Verify independence
	offA, err := store.Get("group-a", "shared-topic", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(50), offA, "group-a offset should be 50")

	offB, err := store.Get("group-b", "shared-topic", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(100), offB, "group-b offset should be 100")
}

// TestOffsetStore_MultiplePartitions verifies that offsets for different
// partitions of the same group+topic are stored independently.
func TestOffsetStore_MultiplePartitions(t *testing.T) {
	dir := t.TempDir()
	store, err := NewOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	partitions := []struct {
		partition int32
		offset    int64
	}{
		{0, 10},
		{1, 20},
		{2, 30},
		{3, 40},
	}

	for _, p := range partitions {
		err := store.Commit("multi-part-group", "topic", p.partition, p.offset)
		require.NoError(t, err, "Commit partition %d should succeed", p.partition)
	}

	for _, p := range partitions {
		offset, err := store.Get("multi-part-group", "topic", p.partition)
		require.NoError(t, err)
		assert.Equal(t, p.offset, offset,
			"partition %d offset should be %d", p.partition, p.offset)
	}

	// Verify other partitions unaffected
	offset, err := store.Get("multi-part-group", "topic", 99)
	require.NoError(t, err)
	assert.Equal(t, int64(-1), offset, "uncommitted partition should return -1")
}

// Suppress lint warning for unused import
var _ = fmt.Sprintf
