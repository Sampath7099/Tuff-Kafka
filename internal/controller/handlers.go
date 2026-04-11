package controller

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Sampath7099/Tuff-Kafka/internal/proto"
	"github.com/Sampath7099/Tuff-Kafka/internal/raft"
	"github.com/vmihailenco/msgpack/v5"
)

// ─── Helpers ─────────────────────────────────────────────────────────────────

func (s *Server) requireLeader() error {
	// nil raftNode = single-node / test mode: treat self as leader.
	if s.raftNode == nil {
		return nil
	}
	if !s.raftNode.IsLeader() {
		leaderAddr := s.raftNode.LeaderID()
		return &NotLeaderError{LeaderAddr: leaderAddr}
	}
	return nil
}

func (s *Server) propose(cmdType raft.CommandType, data interface{}) error {
	encoded, err := msgpack.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal command data: %w", err)
	}
	cmd := raft.Command{Type: cmdType, Data: encoded}
	// Use Raft in production; apply directly in test/single-node mode.
	if s.raftNode == nil {
		return s.state.Apply(cmd)
	}
	return s.raftNode.Propose(context.Background(), cmd)
}

// ─── Client ↔ Controller handlers ─────────────────────────────────────────────

func (s *Server) handleCreateTopic(raw []byte) (interface{}, error) {
	if err := s.requireLeader(); err != nil {
		var forwarded proto.OKResp
		if ferr := s.forwardToLeader(proto.TypeCreateTopic, raw, &forwarded); ferr != nil {
			return nil, ferr
		}
		return forwarded, nil
	}
	var req proto.CreateTopicReq
	if err := proto.Decode(raw, &req); err != nil {
		return nil, err
	}
	if err := s.propose(raft.CmdCreateTopic, struct {
		Topic      string `msgpack:"topic"`
		Partitions int32  `msgpack:"partitions"`
	}{Topic: req.Topic, Partitions: req.Partitions}); err != nil {
		return nil, err
	}
	return proto.OKResp{}, nil
}

func (s *Server) handleMetadata(raw []byte) (interface{}, error) {
	var req proto.MetadataReq
	if err := proto.Decode(raw, &req); err != nil {
		return nil, err
	}
	resp, err := s.state.GetMetadata(req.Topic)
	if err != nil {
		return nil, err
	}
	log.Printf("[DEBUG] handleMetadata(topic=%q): version=%d brokers=%d topics=%v", req.Topic, resp.Version, len(resp.Brokers), resp.Topics)
	return resp, nil
}

func (s *Server) handleJoinGroup(raw []byte) (interface{}, error) {
	if err := s.requireLeader(); err != nil {
		var forwarded proto.JoinGroupResp
		if ferr := s.forwardToLeader(proto.TypeJoinGroup, raw, &forwarded); ferr != nil {
			return nil, ferr
		}
		return forwarded, nil
	}
	var req proto.JoinGroupReq
	if err := proto.Decode(raw, &req); err != nil {
		return nil, err
	}
	if err := s.propose(raft.CmdJoinGroup, struct {
		GroupID   string   `msgpack:"group_id"`
		MemberID  string   `msgpack:"member_id"`
		Topics    []string `msgpack:"topics"`
		SessionMs int32    `msgpack:"session_ms"`
	}{
		GroupID:   req.GroupID,
		MemberID:  req.MemberID,
		Topics:    req.Topics,
		SessionMs: req.SessionMs,
	}); err != nil {
		return nil, err
	}

	// After commit, compute assignment from current state.
	assignment := s.state.computeAssignment(req.GroupID)
	memberAssignment := assignment[req.MemberID]
	if memberAssignment == nil {
		memberAssignment = make(map[string][]int32)
	}

	s.state.mu.RLock()
	var generation int32
	if g, ok := s.state.Groups[req.GroupID]; ok {
		generation = g.Generation
	}
	s.state.mu.RUnlock()

	return proto.JoinGroupResp{
		MemberID:   req.MemberID,
		Assignment: memberAssignment,
		Generation: generation,
	}, nil
}

func (s *Server) handleHeartbeat(raw []byte) (interface{}, error) {
	if err := s.requireLeader(); err != nil {
		var forwarded proto.HeartbeatResp
		if ferr := s.forwardToLeader(proto.TypeHeartbeat, raw, &forwarded); ferr != nil {
			return nil, ferr
		}
		return forwarded, nil
	}
	var req proto.HeartbeatReq
	if err := proto.Decode(raw, &req); err != nil {
		return nil, err
	}

	rebalance := false
	s.state.mu.Lock()
	if g, ok := s.state.Groups[req.GroupID]; ok {
		if m, ok := g.Members[req.MemberID]; ok {
			if m.Generation != req.Generation {
				rebalance = true
			}
			m.LastSeen = time.Now()
		} else {
			// Member not found — needs to rejoin.
			rebalance = true
		}
	} else {
		rebalance = true
	}
	s.state.mu.Unlock()

	return proto.HeartbeatResp{Rebalance: rebalance}, nil
}

func (s *Server) handleLeaveGroup(raw []byte) (interface{}, error) {
	if err := s.requireLeader(); err != nil {
		var forwarded proto.OKResp
		if ferr := s.forwardToLeader(proto.TypeLeaveGroup, raw, &forwarded); ferr != nil {
			return nil, ferr
		}
		return forwarded, nil
	}
	var req proto.LeaveGroupReq
	if err := proto.Decode(raw, &req); err != nil {
		return nil, err
	}
	if err := s.propose(raft.CmdLeaveGroup, struct {
		GroupID  string `msgpack:"group_id"`
		MemberID string `msgpack:"member_id"`
	}{GroupID: req.GroupID, MemberID: req.MemberID}); err != nil {
		return nil, err
	}
	return proto.OKResp{}, nil
}

// ─── Broker → Controller handlers ─────────────────────────────────────────────

func (s *Server) handleBrokerRegister(raw []byte) (interface{}, error) {
	if err := s.requireLeader(); err != nil {
		var forwarded proto.OKResp
		if ferr := s.forwardToLeader(proto.TypeBrokerRegister, raw, &forwarded); ferr != nil {
			return nil, ferr
		}
		return forwarded, nil
	}
	var req proto.BrokerRegisterReq
	if err := proto.Decode(raw, &req); err != nil {
		return nil, err
	}
	if err := s.propose(raft.CmdRegisterBroker, struct {
		ID   string `msgpack:"id"`
		Addr string `msgpack:"addr"`
	}{ID: req.BrokerID, Addr: req.Addr}); err != nil {
		return nil, err
	}
	log.Printf("[DEBUG] handleBrokerRegister: broker %s at %s registered", req.BrokerID, req.Addr)
	return proto.OKResp{}, nil
}

func (s *Server) handleBrokerHeartbeat(raw []byte) (interface{}, error) {
	if err := s.requireLeader(); err != nil {
		var forwarded proto.OKResp
		if ferr := s.forwardToLeader(proto.TypeBrokerHeartbeat, raw, &forwarded); ferr != nil {
			return nil, ferr
		}
		return forwarded, nil
	}
	var req proto.BrokerHeartbeatReq
	if err := proto.Decode(raw, &req); err != nil {
		return nil, err
	}
	// Update broker LastSeen in-memory only (not a critical metadata mutation).
	s.state.mu.Lock()
	if b, ok := s.state.Brokers[req.BrokerID]; ok {
		b.LastSeen = time.Now()
		b.Epoch = req.Epoch
	}
	s.state.mu.Unlock()
	return proto.OKResp{}, nil
}

// ─── Raft RPC pass-through ─────────────────────────────────────────────────────

// handleRaftRequestVote is called when a peer sends us a RequestVote RPC.
// We get the underlying *raft.RaftNode if available.
func (s *Server) handleRaftRequestVote(raw []byte) (interface{}, error) {
	rn, ok := s.raftNode.(*raft.RaftNode)
	if !ok {
		return nil, fmt.Errorf("raft node unavailable")
	}
	var req raft.RequestVoteReq
	if err := msgpack.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	resp := rn.HandleRequestVote(req)
	return resp, nil
}

// handleRaftAppendEntries is called when the leader sends us AppendEntries.
func (s *Server) handleRaftAppendEntries(raw []byte) (interface{}, error) {
	rn, ok := s.raftNode.(*raft.RaftNode)
	if !ok {
		return nil, fmt.Errorf("raft node unavailable")
	}
	var req raft.AppendEntriesReq
	if err := msgpack.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	resp := rn.HandleAppendEntries(req)
	return resp, nil
}

// ─── Broker failure detection ──────────────────────────────────────────────────

// runBrokerFailureDetector periodically scans for dead brokers (leader only).
func (s *Server) runBrokerFailureDetector() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if s.raftNode != nil && s.raftNode.IsLeader() {
			s.detectAndReassign()
		}
	}
}

// detectAndReassign moves dead broker partitions to the least-loaded live broker.
func (s *Server) detectAndReassign() {
	timeout := time.Duration(s.brokerSessionTimeoutMs) * time.Millisecond
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	now := time.Now()
	s.state.mu.RLock()
	var deadIDs []string
	for id, b := range s.state.Brokers {
		if now.Sub(b.LastSeen) > timeout {
			deadIDs = append(deadIDs, id)
		}
	}
	s.state.mu.RUnlock()

	if len(deadIDs) == 0 {
		return
	}

	// Identify live brokers.
	liveIDs := s.state.liveBrokerIDs(timeout)
	if len(liveIDs) == 0 {
		return // nothing to reassign to
	}

	// For each dead broker, collect affected partitions FIRST, then deregister and reassign.
	for i, deadID := range deadIDs {
		// Collect affected partitions before deregistration deletes them!
		s.state.mu.RLock()
		type partitionRef struct {
			topic     string
			partition int32
		}
		var affected []partitionRef
		for topic, ti := range s.state.Topics {
			for p, leader := range ti.Leaders {
				if leader == deadID {
					affected = append(affected, partitionRef{topic: topic, partition: p})
				}
			}
		}
		s.state.mu.RUnlock()

		_ = s.propose(raft.CmdDeregisterBroker, struct {
			ID string `msgpack:"id"`
		}{ID: deadID})

		// Reassign partitions that were owned by deadID.
		for j, ref := range affected {
			newLeader := liveIDs[(i+j)%len(liveIDs)]
			_ = s.propose(raft.CmdAssignLeader, struct {
				Topic     string `msgpack:"topic"`
				Partition int32  `msgpack:"partition"`
				BrokerID  string `msgpack:"broker_id"`
			}{Topic: ref.topic, Partition: ref.partition, BrokerID: newLeader})
		}
	}
}
