// Package storage provides an OffsetStore for persisting consumer group offsets.
//
// File layout: <dir>/<group>/<topic>-<partition>.off
// Each .off file contains a single ASCII integer (the committed offset).
// Concurrent commits to the same file are serialized via per-file background goroutines.
package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type commitReq struct {
	offset int64
	result chan error
}

type offsetFile struct {
	ch   chan commitReq
	done chan struct{}
}

// OffsetStore persists consumer group offsets as flat files.
type OffsetStore struct {
	dir   string
	mu    sync.Mutex
	files map[string]*offsetFile // key: "group/topic-partition"
}

// NewOffsetStore creates a store rooted at dir. Creates dir if needed.
func NewOffsetStore(dir string) (*OffsetStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &OffsetStore{
		dir:   dir,
		files: make(map[string]*offsetFile),
	}, nil
}

// Commit persists offset for (group, topic, partition). Thread-safe.
func (s *OffsetStore) Commit(group, topic string, partition int32, offset int64) error {
	key := fmt.Sprintf("%s/%s-%d", group, topic, partition)
	of := s.getOrCreateFile(key)
	result := make(chan error, 1)
	of.ch <- commitReq{offset: offset, result: result}
	return <-result
}

// Get returns the last committed offset, or -1 if none exists.
func (s *OffsetStore) Get(group, topic string, partition int32) (int64, error) {
	key := fmt.Sprintf("%s/%s-%d", group, topic, partition)
	data, err := os.ReadFile(s.filePath(key))
	if os.IsNotExist(err) {
		return -1, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
}

// Close stops all background writer goroutines.
func (s *OffsetStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, of := range s.files {
		close(of.ch)
		<-of.done
	}
}

func (s *OffsetStore) getOrCreateFile(key string) *offsetFile {
	s.mu.Lock()
	defer s.mu.Unlock()

	if of, ok := s.files[key]; ok {
		return of
	}

	of := &offsetFile{
		ch:   make(chan commitReq, 64),
		done: make(chan struct{}),
	}
	s.files[key] = of
	go s.runWriter(key, of)
	return of
}

func (s *OffsetStore) runWriter(key string, of *offsetFile) {
	path := s.filePath(key)
	for {
		select {
		case req, ok := <-of.ch:
			if !ok {
				close(of.done)
				return
			}
			content := strconv.FormatInt(req.offset, 10)
			err := os.MkdirAll(filepath.Dir(path), 0o755)
			if err == nil {
				err = os.WriteFile(path, []byte(content), 0o644)
			}
			req.result <- err
		}
	}
}

func (s *OffsetStore) filePath(key string) string {
	return filepath.Join(s.dir, key+".off")
}
