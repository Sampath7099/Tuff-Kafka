// Package testenv provides an integration test harness.
// Spins up broker and controller subprocesses on random ports.
package testenv

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Sampath7099/Tuff-Kafka/internal/consumer"
	"github.com/Sampath7099/Tuff-Kafka/internal/producer"
	"github.com/Sampath7099/Tuff-Kafka/internal/proto"
	"gopkg.in/yaml.v3"
)

type process struct {
	id   string
	addr string
	cmd  *exec.Cmd
	mu   sync.Mutex
	dead bool
}

func (p *process) kill() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != nil && p.cmd.Process != nil && !p.dead {
		_ = p.cmd.Process.Kill()
		_ = p.cmd.Wait()
		p.dead = true
	}
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func buildBinaries(t *testing.T) string {
	t.Helper()
	workspacePath := "/home/vishak/IIITH-III-II/ds/project-tuff_cuffka/cuffka"
	tmpDir := t.TempDir()

	// Default to 'go' but could specify full path
	goBin := "/usr/local/go/bin/go"

	cmd1 := exec.Command(goBin, "build", "-o", filepath.Join(tmpDir, "controller"), filepath.Join(workspacePath, "cmd", "controller"))
	cmd1.Dir = workspacePath
	if err := cmd1.Run(); err != nil {
		t.Logf("build controller failed: %v", err)
		return ""
	}

	cmd2 := exec.Command(goBin, "build", "-o", filepath.Join(tmpDir, "broker"), filepath.Join(workspacePath, "cmd", "broker"))
	cmd2.Dir = workspacePath
	if err := cmd2.Run(); err != nil {
		t.Logf("build broker failed: %v", err)
		return ""
	}

	return tmpDir
}

// Cluster represents a running Tuff Cuffka cluster for integration tests.
type Cluster struct {
	t           *testing.T
	BrokerAddrs []string
	CtrlAddrs   []string
	dataDir     string

	controllers []*process
	brokers     []*process
}

// New starts 3 controllers + nBrokers brokers. Registers t.Cleanup for teardown.
func New(t *testing.T, nBrokers int) *Cluster {
	t.Helper()

	binaryDir := buildBinaries(t)
	if binaryDir == "" {
		t.Skip("could not build cuffka binaries, skipping integration test")
		return nil
	}

	dataDir := t.TempDir()

	c := &Cluster{
		t:       t,
		dataDir: dataDir,
	}

	// 3 controllers
	ctrlPeers := make([]struct{ ID, Addr string }, 3)
	for i := 0; i < 3; i++ {
		port := freePort(t)
		ctrlPeers[i] = struct{ ID, Addr string }{
			ID:   fmt.Sprintf("ctrl-%d", i+1),
			Addr: fmt.Sprintf("127.0.0.1:%d", port),
		}
		c.CtrlAddrs = append(c.CtrlAddrs, ctrlPeers[i].Addr)
	}

	for i := 0; i < 3; i++ {
		id := ctrlPeers[i].ID
		addr := ctrlPeers[i].Addr
		raftDir := filepath.Join(dataDir, "raft-"+id)
		os.MkdirAll(raftDir, 0755)

		var peers []map[string]string
		for j, p := range ctrlPeers {
			if j != i {
				peers = append(peers, map[string]string{"id": p.ID, "addr": p.Addr})
			}
		}

		cfgData, _ := yaml.Marshal(map[string]interface{}{
			"id":                          id,
			"addr":                        addr,
			"raft_dir":                    raftDir,
			"peers":                       peers,
			"broker_session_timeout_ms":   3000,
			"consumer_session_timeout_ms": 10000,
		})
		cfgPath := filepath.Join(dataDir, id+".yaml")
		os.WriteFile(cfgPath, cfgData, 0644)

		cmd := exec.Command(filepath.Join(binaryDir, "controller"), "-config", cfgPath)
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start controller %s: %v", id, err)
		}
		c.controllers = append(c.controllers, &process{id: id, addr: addr, cmd: cmd})
	}

	for i := 0; i < nBrokers; i++ {
		port := freePort(t)
		id := fmt.Sprintf("broker-%d", i+1)
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		brokerDataDir := filepath.Join(dataDir, id)
		os.MkdirAll(brokerDataDir, 0755)

		cfgData, _ := yaml.Marshal(map[string]interface{}{
			"id":                    id,
			"addr":                  addr,
			"data_dir":              brokerDataDir,
			"controller_addrs":      c.CtrlAddrs,
			"heartbeat_interval_ms": 500,
			"session_timeout_ms":    3000,
			"fetch_max_wait_ms":     200,
			"fetch_max_bytes":       1048576,
		})
		cfgPath := filepath.Join(dataDir, id+".yaml")
		os.WriteFile(cfgPath, cfgData, 0644)

		cmd := exec.Command(filepath.Join(binaryDir, "broker"), "-config", cfgPath)
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start broker %s: %v", id, err)
		}
		c.BrokerAddrs = append(c.BrokerAddrs, addr)
		c.brokers = append(c.brokers, &process{id: id, addr: addr, cmd: cmd})
	}

	t.Cleanup(c.Shutdown)
	c.waitHealthy(20 * time.Second)
	return c
}

func (c *Cluster) CreateTopic(topic string, partitions int32) {
	c.t.Helper()
	conn := c.dialLeader()
	if conn == nil {
		c.t.Fatalf("CreateTopic: could not reach controller leader")
	}
	defer conn.Close()

	req := proto.CreateTopicReq{Topic: topic, Partitions: partitions}
	if err := proto.WriteFrame(conn, proto.TypeCreateTopic, req); err != nil {
		c.t.Fatalf("CreateTopic write: %v", err)
	}
	msgType, raw, err := proto.ReadFrame(conn)
	if err != nil {
		c.t.Fatalf("CreateTopic read: %v", err)
	}
	if msgType == proto.TypeError {
		var e proto.ErrorResp
		_ = proto.Decode(raw, &e)
		c.t.Fatalf("CreateTopic error: %s", e.Message)
	}
}

func (c *Cluster) NewProducer() *producer.Producer {
	return producer.NewProducer(c.CtrlAddrs)
}

func (c *Cluster) NewConsumer(group, topic string) *consumer.Consumer {
	c.t.Helper()
	cons := consumer.NewConsumer(c.CtrlAddrs, group)
	cons.Subscribe(topic)
	grp := consumer.NewConsumerGroup(cons)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := grp.Join(ctx); err != nil {
		c.t.Fatalf("NewConsumer Join(%s/%s): %v", group, topic, err)
	}
	return cons
}

func (c *Cluster) KillBroker(index int) {
	if index < 0 || index >= len(c.brokers) {
		c.t.Fatalf("KillBroker: index %d out of range", index)
	}
	c.brokers[index].kill()
}

func (c *Cluster) KillControllerLeader() {
	leaderAddr := c.currentLeaderAddr()
	for _, ctrl := range c.controllers {
		if ctrl.addr == leaderAddr {
			ctrl.kill()
			return
		}
	}
}

func (c *Cluster) Shutdown() {
	for _, ctrl := range c.controllers {
		ctrl.kill()
	}
	for _, b := range c.brokers {
		b.kill()
	}
}

func (c *Cluster) waitHealthy(timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		addr := c.currentLeaderAddr()
		if addr == "" {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		req := proto.MetadataReq{Topic: ""}
		_ = proto.WriteFrame(conn, proto.TypeMetadata, req)
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		msgType, raw, err := proto.ReadFrame(conn)
		conn.Close()
		if err != nil || msgType != proto.TypeResponse {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		var resp proto.MetadataResp
		if err := proto.Decode(raw, &resp); err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if len(resp.Brokers) >= len(c.brokers) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	c.t.Fatalf("cluster did not become healthy within %v", timeout)
}

func (c *Cluster) dialLeader() net.Conn {
	for _, addr := range c.CtrlAddrs {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err != nil {
			continue
		}
		req := proto.MetadataReq{Topic: ""}
		if err := proto.WriteFrame(conn, proto.TypeMetadata, req); err != nil {
			conn.Close()
			continue
		}
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		msgType, raw, err := proto.ReadFrame(conn)
		if err != nil {
			conn.Close()
			continue
		}
		if msgType == proto.TypeError {
			var e proto.ErrorResp
			_ = proto.Decode(raw, &e)
			conn.Close()
			continue
		}
		conn.SetReadDeadline(time.Time{})
		return conn
	}
	return nil
}

func (c *Cluster) currentLeaderAddr() string {
	conn := c.dialLeader()
	if conn != nil {
		addr := conn.RemoteAddr().String()
		conn.Close()
		return addr
	}
	return ""
}
