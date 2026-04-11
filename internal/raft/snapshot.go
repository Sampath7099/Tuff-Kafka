package raft

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/vmihailenco/msgpack/v5"
)

// Snapshot is a point-in-time capture of the state machine for log compaction.
type Snapshot struct {
	LastIncludedIndex int64  `msgpack:"last_included_index"`
	LastIncludedTerm  int64  `msgpack:"last_included_term"`
	Data              []byte `msgpack:"data"`
}

// InstallSnapshotReq is the Raft InstallSnapshot RPC.
type InstallSnapshotReq struct {
	Term              int64  `msgpack:"term"`
	LeaderID          string `msgpack:"leader_id"`
	LastIncludedIndex int64  `msgpack:"last_included_index"`
	LastIncludedTerm  int64  `msgpack:"last_included_term"`
	Data              []byte `msgpack:"data"`
}

type InstallSnapshotResp struct {
	Term int64 `msgpack:"term"`
}

// SaveSnapshot atomically writes a snapshot (write-tmp + rename).
func SaveSnapshot(dir string, snap Snapshot) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("SaveSnapshot mkdir: %w", err)
	}
	data, err := msgpack.Marshal(snap)
	if err != nil {
		return fmt.Errorf("SaveSnapshot marshal: %w", err)
	}
	tmpPath := filepath.Join(dir, "snapshot.tmp")
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("SaveSnapshot write: %w", err)
	}
	finalPath := filepath.Join(dir, "snapshot")
	return os.Rename(tmpPath, finalPath)
}

// LoadSnapshot reads the most recent snapshot from dir.
// Returns (zero, nil) if no snapshot exists yet.
func LoadSnapshot(dir string) (Snapshot, error) {
	path := filepath.Join(dir, "snapshot")
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Snapshot{}, nil
		}
		return Snapshot{}, fmt.Errorf("LoadSnapshot read: %w", err)
	}
	var snap Snapshot
	if err := msgpack.Unmarshal(data, &snap); err != nil {
		return Snapshot{}, fmt.Errorf("LoadSnapshot unmarshal: %w", err)
	}
	return snap, nil
}
