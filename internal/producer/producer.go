// Package producer implements the Tuff Cuffka producer client.
package producer

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Sampath7099/Tuff-Kafka/internal/proto"
)

// Producer discovers partition leaders from the controller and sends PRODUCE
// requests directly to the owning broker. Retries with metadata refresh on
// ErrNotLeader, exponential backoff on transient errors.
type Producer struct {
	ctrlAddrs   []string
	mu          sync.RWMutex
	metadata    *proto.MetadataResp
	metaVersion int64
	conns       map[string]interface{}
	rrCounter   int64 // atomic, for round-robin keyless routing
}

func NewProducer(ctrlAddrs []string) *Producer {
	return &Producer{
		ctrlAddrs: ctrlAddrs,
		conns:     make(map[string]interface{}),
	}
}

// Produce sends a message. Key routing: hash(key)%N for keyed, round-robin for keyless.
// Retries up to 5 times with exponential backoff on transient errors (connection
// refused, ErrNotLeader, ErrUnknownTopic).
func (p *Producer) Produce(topic, key string, value []byte) (int64, error) {
	const maxAttempts = 10
	backoff := 500 * time.Millisecond
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff *= 2
			if backoff > 3*time.Second {
				backoff = 3 * time.Second
			}
		}

		if err := p.ensureMetadata(topic); err != nil {
			lastErr = err
			continue
		}

		partition := p.partitionFor(topic, key)
		offset, err := p.sendProduce(topic, partition, key, value)
		if err == nil {
			return offset, nil
		}
		lastErr = err

		// Check if it's a protocol-level error
		var perr proto.ErrorResp
		if e, ok := err.(proto.ErrorResp); ok {
			perr = e
		} else if e, ok := err.(*proto.ErrorResp); ok {
			perr = *e
		}

		isRetryable := perr.Code == proto.ErrNotLeader ||
			perr.Code == proto.ErrUnknownTopic

		// Also retry on raw connection errors (connection refused, broken pipe, etc.)
		if !isRetryable {
			if _, ok := err.(proto.ErrorResp); !ok {
				if _, ok := err.(*proto.ErrorResp); !ok {
					// It's a non-protocol error (dial/write/read failure) — retryable
					isRetryable = true
				}
			}
		}

		if !isRetryable {
			return 0, err
		}

		// Purge cached connection for this broker and force metadata refresh
		p.mu.Lock()
		for addr, raw := range p.conns {
			if conn, ok := raw.(net.Conn); ok {
				_ = conn.Close()
			}
			delete(p.conns, addr)
			_ = addr
		}
		p.metadata = nil // force full refresh
		p.mu.Unlock()
	}

	return 0, fmt.Errorf("produce failed after %d attempts: %w", maxAttempts, lastErr)
}

func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, raw := range p.conns {
		if conn, ok := raw.(net.Conn); ok {
			_ = conn.Close()
		}
		delete(p.conns, addr)
	}
	return nil
}

func (p *Producer) ensureMetadata(topic string) error {
	p.mu.RLock()
	if p.metadata != nil {
		if topicMeta, ok := p.metadata.Topics[topic]; ok && len(topicMeta.Leaders) > 0 {
			p.mu.RUnlock()
			return nil
		}
	}
	p.mu.RUnlock()

	return p.refreshMetadata(topic)
}

func (p *Producer) refreshMetadata(topic string) error {
	req := proto.MetadataReq{Topic: topic}

	for _, addr := range p.ctrlAddrs {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			continue
		}

		_ = proto.WriteFrame(conn, proto.TypeMetadata, req)
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		msgType, raw, err := proto.ReadFrame(conn)
		_ = conn.Close()
		if err != nil {
			continue
		}
		if msgType == proto.TypeError {
			var e proto.ErrorResp
			if decErr := proto.Decode(raw, &e); decErr != nil {
				continue
			}
			continue
		}

		var resp proto.MetadataResp
		if err := proto.Decode(raw, &resp); err != nil {
			continue
		}

		p.mu.Lock()
		p.metadata = &resp
		p.metaVersion = resp.Version
		p.mu.Unlock()
		return nil
	}

	return fmt.Errorf("refreshMetadata: no controller reachable")
}

func (p *Producer) partitionFor(topic, key string) int32 {
	p.mu.RLock()
	if p.metadata == nil {
		p.mu.RUnlock()
		return 0
	}
	topicMeta, ok := p.metadata.Topics[topic]
	if !ok || len(topicMeta.Leaders) == 0 {
		p.mu.RUnlock()
		return 0
	}
	n := int32(len(topicMeta.Leaders))
	p.mu.RUnlock()

	if key == "" {
		return int32(atomic.AddInt64(&p.rrCounter, 1)&0x7fffffff) % n
	}
	return int32(fnv32a(key)&0x7fffffff) % n
}

func (p *Producer) sendProduce(topic string, partition int32, key string, value []byte) (int64, error) {
	p.mu.RLock()
	if p.metadata == nil {
		p.mu.RUnlock()
		return 0, fmt.Errorf("sendProduce: metadata unavailable")
	}
	topicMeta, ok := p.metadata.Topics[topic]
	if !ok {
		p.mu.RUnlock()
		return 0, proto.ErrorResp{Code: proto.ErrUnknownTopic, Message: "unknown topic"}
	}
	brokerID, ok := topicMeta.Leaders[partition]
	if !ok {
		p.mu.RUnlock()
		return 0, proto.ErrorResp{Code: proto.ErrNotLeader, Message: "partition leader unknown"}
	}
	broker, ok := p.metadata.Brokers[brokerID]
	if !ok {
		p.mu.RUnlock()
		return 0, proto.ErrorResp{Code: proto.ErrNotLeader, Message: "broker metadata missing"}
	}
	addr := broker.Addr
	rawConn, ok := p.conns[addr]
	p.mu.RUnlock()

	var conn net.Conn
	if ok {
		if c, isConn := rawConn.(net.Conn); isConn {
			conn = c
		}
	}
	if conn == nil {
		dialed, err := net.Dial("tcp", addr)
		if err != nil {
			return 0, err
		}
		conn = dialed
		p.mu.Lock()
		p.conns[addr] = conn
		p.mu.Unlock()
	}

	req := proto.ProduceReq{Topic: topic, Partition: partition, Key: key, Value: value}
	if err := proto.WriteFrame(conn, proto.TypeProduce, req); err != nil {
		return 0, err
	}

	msgType, raw, err := proto.ReadFrame(conn)
	if err != nil {
		return 0, err
	}
	if msgType == proto.TypeError {
		var eresp proto.ErrorResp
		if err := proto.Decode(raw, &eresp); err != nil {
			return 0, err
		}
		return 0, eresp
	}

	var resp proto.ProduceResp
	if err := proto.Decode(raw, &resp); err != nil {
		return 0, err
	}
	return resp.Offset, nil
}

// fnv32a computes FNV-1a hash for key-based partition routing.
func fnv32a(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}
