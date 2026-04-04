// Package broker implements the data broker process for Tuff Cuffka.
package broker

import (
	"fmt"
	"net"
	"path/filepath"
	"sync"

	"github.com/Sampath7099/Tuff-Kafka/internal/proto"
	"github.com/Sampath7099/Tuff-Kafka/internal/storage"
)

// Config holds broker configuration loaded from YAML.
type Config struct {
	ID                  string   `yaml:"id"`
	Addr                string   `yaml:"addr"`
	DataDir             string   `yaml:"data_dir"`
	ControllerAddrs     []string `yaml:"controller_addrs"`
	HeartbeatIntervalMs int32    `yaml:"heartbeat_interval_ms"`
	SessionTimeoutMs    int32    `yaml:"session_timeout_ms"`
	FetchMaxWaitMs      int32    `yaml:"fetch_max_wait_ms"`
	FetchMaxBytes       int32    `yaml:"fetch_max_bytes"`
}

type logKey struct {
	topic     string
	partition int32
}

type fetchWaiter struct {
	offset   int64
	maxBytes int32
	result   chan fetchResult
}

type fetchResult struct {
	records []storage.LogRecord
	err     error
}

// Broker is the data broker process. It owns partition logs and serves
// PRODUCE, FETCH, and COMMIT requests from clients.
type Broker struct {
	id           string
	addr         string
	dataDir      string
	ctrlAddrs    []string
	logs         map[logKey]*storage.PartitionLog
	offsets      *storage.OffsetStore
	mu           sync.RWMutex
	fetchWaiters map[logKey][]*fetchWaiter
	epoch        int64
	registered   int32
}

func NewBroker(cfg Config) (*Broker, error) {
	offsets, err := storage.NewOffsetStore(filepath.Join(cfg.DataDir, "offsets"))
	if err != nil {
		return nil, err
	}

	return &Broker{
		id:           cfg.ID,
		addr:         cfg.Addr,
		dataDir:      cfg.DataDir,
		ctrlAddrs:    cfg.ControllerAddrs,
		logs:         make(map[logKey]*storage.PartitionLog),
		offsets:      offsets,
		fetchWaiters: make(map[logKey][]*fetchWaiter),
	}, nil
}

func (b *Broker) Start() error {
	ln, err := net.Listen("tcp", b.addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go b.handleConn(conn)
	}
}

func (b *Broker) Shutdown() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for k, l := range b.logs {
		_ = l.Close()
		delete(b.logs, k)
	}
	if b.offsets != nil {
		b.offsets.Close()
	}
	return nil
}

// OpenLog opens or creates the partition log. Idempotent.
func (b *Broker) OpenLog(topic string, partition int32) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	k := logKey{topic: topic, partition: partition}
	if _, exists := b.logs[k]; exists {
		return nil
	}

	l, err := storage.NewPartitionLog(b.dataDir, topic, partition)
	if err != nil {
		return err
	}
	b.logs[k] = l
	return nil
}

func (b *Broker) handleConn(conn interface{}) {
	nc, ok := conn.(net.Conn)
	if !ok {
		return
	}
	defer nc.Close()

	for {
		msgType, raw, err := proto.ReadFrame(nc)
		if err != nil {
			return
		}

		var resp interface{}
		switch msgType {
		case proto.TypeProduce:
			resp, err = b.handleProduce(raw)
		case proto.TypeFetch:
			resp, err = b.handleFetch(raw)
		case proto.TypeCommit:
			resp, err = b.handleCommit(raw)
		default:
			err = fmt.Errorf("unsupported message type: %d", msgType)
		}

		if err != nil {
			_ = proto.WriteFrame(nc, proto.TypeError, proto.ErrorResp{Message: err.Error()})
			continue
		}

		if e, ok := resp.(proto.ErrorResp); ok {
			_ = proto.WriteFrame(nc, proto.TypeError, e)
			continue
		}

		_ = proto.WriteFrame(nc, proto.TypeResponse, resp)
	}
}
