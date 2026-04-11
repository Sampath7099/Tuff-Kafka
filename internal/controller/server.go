package controller

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/Sampath7099/Tuff-Kafka/internal/proto"
	"github.com/Sampath7099/Tuff-Kafka/internal/raft"
)

// Config holds controller configuration loaded from YAML.
type Config struct {
	ID      string       `yaml:"id"`
	Addr    string       `yaml:"addr"`
	RaftDir string       `yaml:"raft_dir"`
	Peers   []PeerConfig `yaml:"peers"`

	BrokerSessionTimeoutMs   int32 `yaml:"broker_session_timeout_ms"`
	ConsumerSessionTimeoutMs int32 `yaml:"consumer_session_timeout_ms"`
}

type PeerConfig struct {
	ID   string `yaml:"id"`
	Addr string `yaml:"addr"`
}

// NotLeaderError is returned when a request reaches a non-leader controller.
type NotLeaderError struct {
	LeaderAddr string
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("not the controller leader, redirect to %s", e.LeaderAddr)
}

func (e NotLeaderError) IsError() int32 { return proto.ErrNotController }

// RaftIface abstracts the RaftNode for testability.
type RaftIface interface {
	IsLeader() bool
	LeaderID() string
	Propose(ctx context.Context, cmd raft.Command) error
}

// Server is the controller TCP server.
type Server struct {
	id    string
	addr  string
	state *ClusterState

	raftNode RaftIface
	peers    []string
	peerAddr map[string]string // controllerID -> tcp address

	brokerSessionTimeoutMs   int32
	consumerSessionTimeoutMs int32

	ln net.Listener
}

func NewServer(cfg Config, state *ClusterState, raftNode RaftIface) *Server {
	peers := make([]string, 0, len(cfg.Peers))
	peerAddr := make(map[string]string, len(cfg.Peers)+1)
	for _, p := range cfg.Peers {
		peers = append(peers, p.Addr)
		peerAddr[p.ID] = p.Addr
	}
	peerAddr[cfg.ID] = cfg.Addr

	brokerTimeout := cfg.BrokerSessionTimeoutMs
	if brokerTimeout == 0 {
		brokerTimeout = 10000
	}
	consumerTimeout := cfg.ConsumerSessionTimeoutMs
	if consumerTimeout == 0 {
		consumerTimeout = 10000
	}

	return &Server{
		id:                       cfg.ID,
		addr:                     cfg.Addr,
		state:                    state,
		raftNode:                 raftNode,
		peers:                    peers,
		peerAddr:                 peerAddr,
		brokerSessionTimeoutMs:   brokerTimeout,
		consumerSessionTimeoutMs: consumerTimeout,
	}
}

func (s *Server) leaderAddr() string {
	if s.raftNode == nil {
		return s.addr
	}
	leaderID := s.raftNode.LeaderID()
	if leaderID == "" {
		return ""
	}
	if leaderID == s.id {
		return s.addr
	}
	if addr, ok := s.peerAddr[leaderID]; ok {
		return addr
	}
	// Fallback: some implementations may expose the leader address directly.
	return leaderID
}

func (s *Server) writeRawFrame(conn net.Conn, msgType proto.MessageType, raw []byte) error {
	frame := make([]byte, 1+len(raw))
	frame[0] = byte(msgType)
	copy(frame[1:], raw)

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(frame)))
	if _, err := conn.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := conn.Write(frame)
	return err
}

func (s *Server) forwardToLeader(msgType proto.MessageType, raw []byte, out interface{}) error {
	leaderAddr := s.leaderAddr()
	if leaderAddr == "" || leaderAddr == s.addr {
		return &NotLeaderError{LeaderAddr: leaderAddr}
	}

	conn, err := net.DialTimeout("tcp", leaderAddr, 1500*time.Millisecond)
	if err != nil {
		return fmt.Errorf("forward to leader %s: %w", leaderAddr, err)
	}
	defer conn.Close()

	if err := s.writeRawFrame(conn, msgType, raw); err != nil {
		return err
	}

	rType, rRaw, err := proto.ReadFrame(conn)
	if err != nil {
		return err
	}
	if rType == proto.TypeError {
		var e proto.ErrorResp
		if err := proto.Decode(rRaw, &e); err != nil {
			return err
		}
		return e
	}

	if out == nil {
		return nil
	}
	return proto.Decode(rRaw, out)
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("controller listen: %w", err)
	}
	s.ln = ln

	go s.runBrokerFailureDetector()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err // Shutdown() calls ln.Close() which unblocks Accept
		}
		go s.handleConn(conn)
	}
}

func (s *Server) Shutdown() error {
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}

func (s *Server) handleConn(conn interface{}) {
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
		case proto.TypeCreateTopic:
			resp, err = s.handleCreateTopic(raw)
		case proto.TypeMetadata:
			resp, err = s.handleMetadata(raw)
		case proto.TypeJoinGroup:
			resp, err = s.handleJoinGroup(raw)
		case proto.TypeHeartbeat:
			resp, err = s.handleHeartbeat(raw)
		case proto.TypeLeaveGroup:
			resp, err = s.handleLeaveGroup(raw)
		case proto.TypeBrokerRegister:
			resp, err = s.handleBrokerRegister(raw)
		case proto.TypeBrokerHeartbeat:
			resp, err = s.handleBrokerHeartbeat(raw)
		case proto.TypeRaftRequestVote:
			resp, err = s.handleRaftRequestVote(raw)
		case proto.TypeRaftAppendEntries:
			resp, err = s.handleRaftAppendEntries(raw)
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
