// Package storage provides durable partition logs for Tuff Cuffka.
//
// Record format (each record in the file):
//
//	[2B key_len BE] [4B value_len BE] [key bytes] [value bytes]
//
// Offset is the record's position in the in-memory index (0-based, monotonic).
package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// LogRecord is a single record read from the log.
type LogRecord struct {
	Offset int64
	Key    string
	Value  []byte
}

// PartitionLog is an append-only log file with an in-memory offset index.
// All methods are safe for concurrent use.
type PartitionLog struct {
	mu      sync.RWMutex
	file    *os.File
	index   []int64 // index[offset] = byte position in file
	nextOff int64
}

// NewPartitionLog opens or creates the log at <dir>/<topic>-<partition>.log.
// Rebuilds the offset index from the file on open.
func NewPartitionLog(dir, topic string, partition int32) (*PartitionLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, fmt.Sprintf("%s-%d.log", topic, partition))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	l := &PartitionLog{file: f}
	if err := l.rebuildIndex(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return l, nil
}

// Append writes key+value as a new record. Returns the assigned offset.
func (l *PartitionLog) Append(key string, value []byte) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	keyBytes := []byte(key)
	rec := make([]byte, 2+4+len(keyBytes)+len(value))
	binary.BigEndian.PutUint16(rec[0:2], uint16(len(keyBytes)))
	binary.BigEndian.PutUint32(rec[2:6], uint32(len(value)))
	copy(rec[6:], keyBytes)
	copy(rec[6+len(keyBytes):], value)

	fi, err := l.file.Stat()
	if err != nil {
		return 0, err
	}
	pos := fi.Size()

	if _, err := l.file.Write(rec); err != nil {
		return 0, err
	}

	offset := l.nextOff
	l.index = append(l.index, pos)
	l.nextOff++
	return offset, nil
}

// Read returns the key and value at the given offset.
func (l *PartitionLog) Read(offset int64) (string, []byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if offset < 0 || offset >= l.nextOff {
		return "", nil, fmt.Errorf("offset %d out of range [0, %d)", offset, l.nextOff)
	}

	return l.readAt(l.index[offset])
}

// ReadBatch returns records starting at offset, up to maxBytes total.
// At least one record is always returned even if it exceeds maxBytes.
func (l *PartitionLog) ReadBatch(offset int64, maxBytes int) ([]LogRecord, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if offset < 0 || offset > l.nextOff {
		return nil, fmt.Errorf("offset %d out of range", offset)
	}

	var records []LogRecord
	var totalBytes int

	for off := offset; off < l.nextOff; off++ {
		key, val, err := l.readAt(l.index[off])
		if err != nil {
			return nil, err
		}
		records = append(records, LogRecord{Offset: off, Key: key, Value: val})
		totalBytes += len(key) + len(val)
		if totalBytes >= maxBytes && len(records) >= 1 {
			break
		}
	}

	return records, nil
}

// NextOffset returns the offset that will be assigned to the next Append.
func (l *PartitionLog) NextOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextOff
}

// Close closes the underlying file.
func (l *PartitionLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}

// rebuildIndex scans the file and populates the in-memory index.
func (l *PartitionLog) rebuildIndex() error {
	l.index = nil
	l.nextOff = 0

	var pos int64
	for {
		var klBuf [2]byte
		if _, err := l.file.ReadAt(klBuf[:], pos); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			break
		}
		keyLen := int64(binary.BigEndian.Uint16(klBuf[:]))

		var vlBuf [4]byte
		if _, err := l.file.ReadAt(vlBuf[:], pos+2); err != nil {
			return fmt.Errorf("rebuildIndex: corrupt at pos %d", pos)
		}
		valLen := int64(binary.BigEndian.Uint32(vlBuf[:]))

		l.index = append(l.index, pos)
		l.nextOff++
		pos += 2 + 4 + keyLen + valLen
	}

	return nil
}

func (l *PartitionLog) readAt(pos int64) (string, []byte, error) {
	var klBuf [2]byte
	if _, err := l.file.ReadAt(klBuf[:], pos); err != nil {
		return "", nil, err
	}
	keyLen := int(binary.BigEndian.Uint16(klBuf[:]))

	var vlBuf [4]byte
	if _, err := l.file.ReadAt(vlBuf[:], pos+2); err != nil {
		return "", nil, err
	}
	valLen := int(binary.BigEndian.Uint32(vlBuf[:]))

	keyBytes := make([]byte, keyLen)
	if _, err := l.file.ReadAt(keyBytes, pos+6); err != nil {
		return "", nil, err
	}

	val := make([]byte, valLen)
	if _, err := l.file.ReadAt(val, pos+6+int64(keyLen)); err != nil {
		return "", nil, err
	}

	return string(keyBytes), val, nil
}

var _ = os.OpenFile
var _ = fmt.Sprintf
