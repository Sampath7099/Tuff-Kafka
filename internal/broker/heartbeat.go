package broker

import (
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/Sampath7099/Tuff-Kafka/internal/proto"
)

// RunHeartbeat sends periodic BROKER_HEARTBEAT to the controller leader.
func (b *Broker) RunHeartbeat(interval interface{}) {
	d, ok := interval.(time.Duration)
	if !ok || d <= 0 {
		d = 3 * time.Second
	}
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	for range ticker.C {
		b.sendHeartbeatToLeader()
	}
}

func (b *Broker) sendHeartbeatToLeader() {
	if atomic.LoadInt32(&b.registered) == 0 {
		if err := b.Register(); err == nil {
			// Successfully registered
		}
		return
	}

	conn, err := b.dialLeader()
	if err != nil {
		return
	}
	defer conn.Close()

	b.mu.RLock()
	epoch := b.epoch
	b.mu.RUnlock()

	req := proto.BrokerHeartbeatReq{BrokerID: b.id, Epoch: epoch}
	if err := proto.WriteFrame(conn, proto.TypeBrokerHeartbeat, req); err != nil {
		return
	}
	// Read and discard response.
	_, _, _ = proto.ReadFrame(conn)
}

// Register sends BROKER_REGISTER to the controller on startup.
func (b *Broker) Register() error {
	conn, err := b.dialLeader()
	if err != nil {
		return fmt.Errorf("broker Register: dial: %w", err)
	}
	defer conn.Close()

	req := proto.BrokerRegisterReq{BrokerID: b.id, Addr: b.addr}
	if err := proto.WriteFrame(conn, proto.TypeBrokerRegister, req); err != nil {
		return fmt.Errorf("broker Register: write: %w", err)
	}

	msgType, raw, err := proto.ReadFrame(conn)
	if err != nil {
		return fmt.Errorf("broker Register: read: %w", err)
	}
	if msgType == proto.TypeError {
		var e proto.ErrorResp
		_ = proto.Decode(raw, &e)
		return fmt.Errorf("broker Register: controller error: %s", e.Message)
	}

	b.mu.Lock()
	if b.epoch == 0 {
		b.epoch = time.Now().UnixNano()
	}
	b.mu.Unlock()
	atomic.StoreInt32(&b.registered, 1)

	return nil
}

// dialLeader tries each controller address and returns the first connection
// that is accepted by the Raft leader (i.e., responds to a Metadata probe without TypeError).
func (b *Broker) dialLeader() (net.Conn, error) {
	req := proto.MetadataReq{Topic: ""}
	for _, addr := range b.ctrlAddrs {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			continue
		}

		if err := proto.WriteFrame(conn, proto.TypeMetadata, req); err != nil {
			conn.Close()
			continue
		}
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		msgType, _, err := proto.ReadFrame(conn)
		if err != nil || msgType == proto.TypeError {
			conn.Close()
			continue
		}

		conn.SetReadDeadline(time.Time{})
		return conn, nil
	}
	return nil, fmt.Errorf("dialLeader: no reachable controller leader in %v", b.ctrlAddrs)
}
