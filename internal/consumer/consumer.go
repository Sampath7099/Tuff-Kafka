// Package consumer implements the Tuff Cuffka consumer client.
package consumer

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/Sampath7099/Tuff-Kafka/internal/proto"
)

// Record is a single consumed message.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Value     []byte
}

type offsetKey struct {
	topic     string
	partition int32
}

// Consumer fetches messages from partition leaders and tracks read offsets.
type Consumer struct {
	brokerConns      map[string]interface{}
	ctrlAddrs        []string
	metadata         *proto.MetadataResp
	groupID          string
	memberID         string
	generation       int32
	assignment       map[string][]int32 // topic → partitions
	offsets          map[offsetKey]int64
	subscribedTopics []string
	mu               sync.Mutex
}

func NewConsumer(ctrlAddrs []string, groupID string) *Consumer {
	return &Consumer{
		brokerConns:      make(map[string]interface{}),
		ctrlAddrs:        ctrlAddrs,
		groupID:          groupID,
		assignment:       make(map[string][]int32),
		offsets:          make(map[offsetKey]int64),
		subscribedTopics: make([]string, 0),
	}
}

// Poll fetches up to maxRecords from all assigned partitions.
// Subscribe adds a topic to the consumer's subscription list.
func (c *Consumer) Subscribe(topic string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.subscribedTopics {
		if t == topic {
			return
		}
	}
	c.subscribedTopics = append(c.subscribedTopics, topic)
}

func (c *Consumer) Poll(ctx context.Context, maxRecords int) ([]Record, error) {
	c.mu.Lock()
	assignment := make(map[string][]int32, len(c.assignment))
	for topic, partitions := range c.assignment {
		copied := make([]int32, len(partitions))
		copy(copied, partitions)
		assignment[topic] = copied
	}
	offsets := make(map[offsetKey]int64, len(c.offsets))
	for k, v := range c.offsets {
		offsets[k] = v
	}
	c.mu.Unlock()

	if len(assignment) == 0 {
		return []Record{}, nil
	}

	results := make([]Record, 0, maxRecords)
	for topic, partitions := range assignment {
		for _, partition := range partitions {
			select {
			case <-ctx.Done():
				return results, ctx.Err()
			default:
			}

			k := offsetKey{topic: topic, partition: partition}
			resp, err := c.fetch(topic, partition, offsets[k], 1<<20, 250)
			if err != nil {
				return results, err
			}

			for _, m := range resp.Messages {
				results = append(results, Record{
					Topic:     topic,
					Partition: partition,
					Offset:    m.Offset,
					Key:       m.Key,
					Value:     m.Value,
				})
				next := m.Offset + 1
				offsets[k] = next
				c.mu.Lock()
				c.offsets[k] = next
				c.mu.Unlock()
				if maxRecords > 0 && len(results) >= maxRecords {
					return results, nil
				}
			}
		}
	}

	return results, nil
}

// CommitAll commits all current local offsets.
func (c *Consumer) CommitAll() error {
	c.mu.Lock()
	offsets := make(map[offsetKey]int64, len(c.offsets))
	for k, v := range c.offsets {
		offsets[k] = v
	}
	groupID := c.groupID
	c.mu.Unlock()

	for k, offset := range offsets {
		if err := c.commit(groupID, k.topic, k.partition, offset); err != nil {
			return err
		}
	}

	return nil
}

// SeekTo resets the local read cursor for (topic, partition).
func (c *Consumer) SeekTo(topic string, partition int32, offset int64) {
	c.mu.Lock()
	c.offsets[offsetKey{topic: topic, partition: partition}] = offset
	c.mu.Unlock()
}

func (c *Consumer) Close() error { return nil }

func (c *Consumer) fetch(topic string, partition int32, offset int64, maxBytes, maxWaitMs int32) (*proto.FetchResp, error) {
	if err := c.refreshMetadata(topic); err != nil {
		return nil, err
	}

	c.mu.Lock()
	meta := c.metadata
	c.mu.Unlock()
	if meta == nil {
		return nil, fmt.Errorf("fetch: metadata unavailable")
	}

	topicMeta, ok := meta.Topics[topic]
	if !ok {
		return nil, proto.ErrorResp{Code: proto.ErrUnknownTopic, Message: "unknown topic"}
	}
	brokerID, ok := topicMeta.Leaders[partition]
	if !ok {
		return nil, proto.ErrorResp{Code: proto.ErrNotLeader, Message: "leader unknown"}
	}
	broker, ok := meta.Brokers[brokerID]
	if !ok {
		return nil, proto.ErrorResp{Code: proto.ErrNotLeader, Message: "broker metadata missing"}
	}

	conn, err := c.getOrDialBroker(broker.Addr)
	if err != nil {
		return nil, err
	}

	req := proto.FetchReq{Topic: topic, Partition: partition, Offset: offset, MaxBytes: maxBytes, MaxWaitMs: maxWaitMs}
	if err := proto.WriteFrame(conn, proto.TypeFetch, req); err != nil {
		return nil, err
	}

	msgType, raw, err := proto.ReadFrame(conn)
	if err != nil {
		return nil, err
	}
	if msgType == proto.TypeError {
		var e proto.ErrorResp
		if err := proto.Decode(raw, &e); err != nil {
			return nil, err
		}
		return nil, e
	}

	var resp proto.FetchResp
	if err := proto.Decode(raw, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Consumer) commit(groupID, topic string, partition int32, offset int64) error {
	if err := c.refreshMetadata(topic); err != nil {
		return err
	}

	c.mu.Lock()
	meta := c.metadata
	c.mu.Unlock()
	if meta == nil {
		return fmt.Errorf("commit: metadata unavailable")
	}
	topicMeta, ok := meta.Topics[topic]
	if !ok {
		return proto.ErrorResp{Code: proto.ErrUnknownTopic, Message: "unknown topic"}
	}
	brokerID, ok := topicMeta.Leaders[partition]
	if !ok {
		return proto.ErrorResp{Code: proto.ErrNotLeader, Message: "leader unknown"}
	}
	broker, ok := meta.Brokers[brokerID]
	if !ok {
		return proto.ErrorResp{Code: proto.ErrNotLeader, Message: "broker metadata missing"}
	}

	conn, err := c.getOrDialBroker(broker.Addr)
	if err != nil {
		return err
	}

	req := proto.CommitReq{Group: groupID, Topic: topic, Partition: partition, Offset: offset}
	if err := proto.WriteFrame(conn, proto.TypeCommit, req); err != nil {
		return err
	}

	msgType, raw, err := proto.ReadFrame(conn)
	if err != nil {
		return err
	}
	if msgType == proto.TypeError {
		var e proto.ErrorResp
		if err := proto.Decode(raw, &e); err != nil {
			return err
		}
		return e
	}

	var okResp proto.OKResp
	if err := proto.Decode(raw, &okResp); err != nil {
		return err
	}
	return nil
}

func (c *Consumer) fetchCommittedOffset(groupID, topic string, partition int32) (int64, error) {
	if err := c.refreshMetadata(topic); err != nil {
		return -1, err
	}

	c.mu.Lock()
	meta := c.metadata
	c.mu.Unlock()
	if meta == nil {
		return -1, fmt.Errorf("fetchCommittedOffset: metadata unavailable")
	}
	topicMeta, ok := meta.Topics[topic]
	if !ok {
		return -1, nil
	}
	brokerID, ok := topicMeta.Leaders[partition]
	if !ok {
		return -1, nil
	}
	broker, ok := meta.Brokers[brokerID]
	if !ok {
		return -1, nil
	}

	conn, err := c.getOrDialBroker(broker.Addr)
	if err != nil {
		return -1, err
	}

	req := proto.CommitReq{Group: groupID, Topic: topic, Partition: partition, Offset: -1}
	if err := proto.WriteFrame(conn, proto.TypeCommit, req); err != nil {
		return -1, err
	}

	msgType, raw, err := proto.ReadFrame(conn)
	if err != nil {
		return -1, err
	}
	if msgType == proto.TypeError {
		var e proto.ErrorResp
		if err := proto.Decode(raw, &e); err != nil {
			return -1, err
		}
		if e.Code == proto.ErrUnknownTopic {
			return -1, nil
		}
		return -1, e
	}

	var resp proto.ProduceResp
	if err := proto.Decode(raw, &resp); err != nil {
		return -1, err
	}
	return resp.Offset, nil
}

func (c *Consumer) dialControllerLeader() (interface{}, error) {
	for _, addr := range c.ctrlAddrs {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			return conn, nil
		}
	}
	return nil, fmt.Errorf("dialControllerLeader: no controller reachable")
}

func (c *Consumer) refreshMetadata(topic string) error {
	connRaw, err := c.dialControllerLeader()
	if err != nil {
		return err
	}
	conn, ok := connRaw.(net.Conn)
	if !ok {
		return fmt.Errorf("refreshMetadata: invalid controller connection")
	}
	defer conn.Close()

	req := proto.MetadataReq{Topic: topic}
	if err := proto.WriteFrame(conn, proto.TypeMetadata, req); err != nil {
		return err
	}

	msgType, raw, err := proto.ReadFrame(conn)
	if err != nil {
		return err
	}
	if msgType == proto.TypeError {
		var e proto.ErrorResp
		if err := proto.Decode(raw, &e); err != nil {
			return err
		}
		return e
	}

	var resp proto.MetadataResp
	if err := proto.Decode(raw, &resp); err != nil {
		return err
	}

	c.mu.Lock()
	c.metadata = &resp
	c.mu.Unlock()
	return nil
}

func (c *Consumer) getOrDialBroker(addr string) (net.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if raw, ok := c.brokerConns[addr]; ok {
		if conn, ok := raw.(net.Conn); ok {
			return conn, nil
		}
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c.brokerConns[addr] = conn
	return conn, nil
}
