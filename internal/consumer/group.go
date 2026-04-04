package consumer

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/Sampath7099/Tuff-Kafka/internal/proto"
)

// ConsumerGroup manages group membership, heartbeats, and rebalancing.
type ConsumerGroup struct {
	consumer *Consumer
	stopCh   chan struct{}
}

func NewConsumerGroup(consumer *Consumer) *ConsumerGroup {
	return &ConsumerGroup{
		consumer: consumer,
		stopCh:   make(chan struct{}),
	}
}

// Join registers with the controller and receives partition assignment.
func (g *ConsumerGroup) Join(ctx context.Context) error {
	connRaw, err := g.consumer.dialControllerLeader()
	if err != nil {
		return err
	}
	conn, ok := connRaw.(net.Conn)
	if !ok {
		return fmt.Errorf("Join: invalid controller connection")
	}
	defer conn.Close()

	g.consumer.mu.Lock()
	req := proto.JoinGroupReq{
		GroupID:   g.consumer.groupID,
		MemberID:  g.consumer.memberID,
		Topics:    append([]string(nil), g.consumer.subscribedTopics...),
		SessionMs: 10000,
	}
	g.consumer.mu.Unlock()

	if err := proto.WriteFrame(conn, proto.TypeJoinGroup, req); err != nil {
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

	var resp proto.JoinGroupResp
	if err := proto.Decode(raw, &resp); err != nil {
		return err
	}

	g.consumer.mu.Lock()
	g.consumer.memberID = resp.MemberID
	g.consumer.generation = resp.Generation
	g.consumer.assignment = resp.Assignment
	g.consumer.mu.Unlock()

	for topic, partitions := range resp.Assignment {
		for _, partition := range partitions {
			offset, err := g.consumer.fetchCommittedOffset(g.consumer.groupID, topic, partition)
			if err != nil {
				return err
			}
			if offset < 0 {
				offset = 0
			}
			g.consumer.SeekTo(topic, partition, offset)
		}
	}

	go g.RunHeartbeat(ctx)
	return nil
}

// Leave sends LEAVE_GROUP to the controller.
func (g *ConsumerGroup) Leave(ctx context.Context) error {
	connRaw, err := g.consumer.dialControllerLeader()
	if err != nil {
		return err
	}
	conn, ok := connRaw.(net.Conn)
	if !ok {
		return fmt.Errorf("Leave: invalid controller connection")
	}
	defer conn.Close()

	g.consumer.mu.Lock()
	req := struct {
		GroupID  string `msgpack:"group_id"`
		MemberID string `msgpack:"member_id"`
	}{
		GroupID:  g.consumer.groupID,
		MemberID: g.consumer.memberID,
	}
	g.consumer.mu.Unlock()

	if err := proto.WriteFrame(conn, proto.TypeLeaveGroup, req); err != nil {
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

	close(g.stopCh)
	return nil
}

// RunHeartbeat periodically sends HEARTBEAT. Re-joins on Rebalance=true.
func (g *ConsumerGroup) RunHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-g.stopCh:
			return
		case <-ticker.C:
			rebalance, err := g.sendHeartbeat()
			if err != nil {
				continue
			}
			if rebalance {
				_ = g.Join(ctx)
			}
		}
	}
}

func (g *ConsumerGroup) Stop() { close(g.stopCh) }

func (g *ConsumerGroup) sendHeartbeat() (bool, error) {
	connRaw, err := g.consumer.dialControllerLeader()
	if err != nil {
		return false, err
	}
	conn, ok := connRaw.(net.Conn)
	if !ok {
		return false, fmt.Errorf("sendHeartbeat: invalid controller connection")
	}
	defer conn.Close()

	g.consumer.mu.Lock()
	req := proto.HeartbeatReq{
		GroupID:    g.consumer.groupID,
		MemberID:   g.consumer.memberID,
		Generation: g.consumer.generation,
	}
	g.consumer.mu.Unlock()

	if err := proto.WriteFrame(conn, proto.TypeHeartbeat, req); err != nil {
		return false, err
	}

	msgType, raw, err := proto.ReadFrame(conn)
	if err != nil {
		return false, err
	}
	if msgType == proto.TypeError {
		var e proto.ErrorResp
		if err := proto.Decode(raw, &e); err != nil {
			return false, err
		}
		return false, e
	}

	var resp proto.HeartbeatResp
	if err := proto.Decode(raw, &resp); err != nil {
		return false, err
	}
	return resp.Rebalance, nil
}
