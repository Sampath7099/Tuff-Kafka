// Package controller implements the metadata state machine for Tuff Cuffka.
// ClusterState implements raft.StateMachine — Apply must be deterministic.
package controller

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Sampath7099/Tuff-Kafka/internal/proto"
	"github.com/Sampath7099/Tuff-Kafka/internal/raft"
	"github.com/vmihailenco/msgpack/v5"
)

type BrokerMeta struct {
	ID       string
	Addr     string
	LastSeen time.Time
	Epoch    int64
}

type TopicInfo struct {
	Partitions int32
	Leaders    map[int32]string // partition → brokerID
}

type ConsumerMember struct {
	MemberID   string
	Topics     []string
	LastSeen   time.Time
	SessionMs  int32
	Generation int32
}

type GroupState struct {
	Members    map[string]*ConsumerMember
	Generation int32
}

// ClusterState is the Raft state machine for cluster metadata.
type ClusterState struct {
	mu sync.RWMutex

	Brokers         map[string]*BrokerMeta
	Topics          map[string]*TopicInfo
	Groups          map[string]*GroupState
	MetadataVersion int64
}

func NewClusterState() *ClusterState {
	return &ClusterState{
		Brokers: make(map[string]*BrokerMeta),
		Topics:  make(map[string]*TopicInfo),
		Groups:  make(map[string]*GroupState),
	}
}

// Apply dispatches a committed Raft command to the appropriate handler.
// Must be deterministic — no I/O, no random, no time.Now inside sub-handlers.
func (s *ClusterState) Apply(cmd raft.Command) error {
	switch cmd.Type {
	case raft.CmdRegisterBroker:
		return s.applyRegisterBroker(cmd.Data)
	case raft.CmdDeregisterBroker:
		return s.applyDeregisterBroker(cmd.Data)
	case raft.CmdCreateTopic:
		return s.applyCreateTopic(cmd.Data)
	case raft.CmdAssignLeader:
		return s.applyAssignLeader(cmd.Data)
	case raft.CmdJoinGroup:
		return s.applyJoinGroup(cmd.Data)
	case raft.CmdLeaveGroup:
		return s.applyLeaveGroup(cmd.Data)
	default:
		return fmt.Errorf("unknown command type %d", cmd.Type)
	}
}

// GetMetadata returns a metadata snapshot. Empty topic = all topics.
func (s *ClusterState) GetMetadata(topic string) (proto.MetadataResp, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	resp := proto.MetadataResp{
		Version: s.MetadataVersion,
		Topics:  make(map[string]proto.TopicMeta),
		Brokers: make(map[string]proto.BrokerInfo),
	}

	for id, b := range s.Brokers {
		resp.Brokers[id] = proto.BrokerInfo{ID: b.ID, Addr: b.Addr}
	}

	if topic == "" {
		for name, t := range s.Topics {
			leaders := make(map[int32]string, len(t.Leaders))
			for p, b := range t.Leaders {
				leaders[p] = b
			}
			resp.Topics[name] = proto.TopicMeta{Partitions: t.Partitions, Leaders: leaders}
		}
	} else {
		t, ok := s.Topics[topic]
		if !ok {
			return proto.MetadataResp{}, fmt.Errorf("unknown topic %q", topic)
		}
		leaders := make(map[int32]string, len(t.Leaders))
		for p, b := range t.Leaders {
			leaders[p] = b
		}
		resp.Topics[topic] = proto.TopicMeta{Partitions: t.Partitions, Leaders: leaders}
	}
	return resp, nil
}

// liveBrokerIDs returns broker IDs whose LastSeen is within timeout.
func (s *ClusterState) liveBrokerIDs(timeout time.Duration) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	var ids []string
	for id, b := range s.Brokers {
		if now.Sub(b.LastSeen) < timeout {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids) // deterministic ordering
	return ids
}

// computeAssignment returns memberID → (topic → []partition) using round-robin.
func (s *ClusterState) computeAssignment(groupID string) map[string]map[string][]int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	g, ok := s.Groups[groupID]
	if !ok || len(g.Members) == 0 {
		return nil
	}

	// Sorted member IDs for determinism.
	memberIDs := make([]string, 0, len(g.Members))
	for id := range g.Members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)

	// Build flat list of all (topic, partition) pairs across all subscribed topics.
	type tpair struct {
		topic     string
		partition int32
	}
	var pairs []tpair

	// Collect all distinct subscribed topics across all members.
	topicSet := make(map[string]struct{})
	for _, m := range g.Members {
		for _, t := range m.Topics {
			topicSet[t] = struct{}{}
		}
	}
	sortedTopics := make([]string, 0, len(topicSet))
	for t := range topicSet {
		sortedTopics = append(sortedTopics, t)
	}
	sort.Strings(sortedTopics)

	for _, topic := range sortedTopics {
		ti, ok := s.Topics[topic]
		if !ok {
			continue
		}
		for p := int32(0); p < ti.Partitions; p++ {
			pairs = append(pairs, tpair{topic: topic, partition: p})
		}
	}

	// Initialise result map.
	result := make(map[string]map[string][]int32, len(memberIDs))
	for _, mid := range memberIDs {
		result[mid] = make(map[string][]int32)
	}

	// Round-robin assign pairs to members.
	for i, pair := range pairs {
		mid := memberIDs[i%len(memberIDs)]
		result[mid][pair.topic] = append(result[mid][pair.topic], pair.partition)
	}
	return result
}

// ─── Internal apply handlers ────────────────────────────────────────────────

type registerBrokerData struct {
	ID   string `msgpack:"id"`
	Addr string `msgpack:"addr"`
}

func (s *ClusterState) applyRegisterBroker(data []byte) error {
	var d registerBrokerData
	if err := msgpack.Unmarshal(data, &d); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.Brokers[d.ID]; !exists {
		s.Brokers[d.ID] = &BrokerMeta{ID: d.ID, Addr: d.Addr}
	} else {
		s.Brokers[d.ID].Addr = d.Addr
	}
	s.Brokers[d.ID].LastSeen = time.Now()
	s.MetadataVersion++
	return nil
}

type deregisterBrokerData struct {
	ID string `msgpack:"id"`
}

func (s *ClusterState) applyDeregisterBroker(data []byte) error {
	var d deregisterBrokerData
	if err := msgpack.Unmarshal(data, &d); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.Brokers, d.ID)
	// Remove as leader from any partitions.
	for _, t := range s.Topics {
		for p, leader := range t.Leaders {
			if leader == d.ID {
				delete(t.Leaders, p)
			}
		}
	}
	s.MetadataVersion++
	return nil
}

type createTopicData struct {
	Topic      string `msgpack:"topic"`
	Partitions int32  `msgpack:"partitions"`
}

func (s *ClusterState) applyCreateTopic(data []byte) error {
	var d createTopicData
	if err := msgpack.Unmarshal(data, &d); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Idempotent — ignore duplicate creates.
	if _, exists := s.Topics[d.Topic]; exists {
		return nil
	}

	// Collect live brokers (last seen within 30s for the purpose of initial assignment).
	now := time.Now()
	var liveIDs []string
	for id, b := range s.Brokers {
		if now.Sub(b.LastSeen) < 30*time.Second {
			liveIDs = append(liveIDs, id)
		}
	}
	if len(liveIDs) == 0 {
		return fmt.Errorf("no live brokers available to assign partitions")
	}
	sort.Strings(liveIDs)

	leaders := make(map[int32]string, d.Partitions)
	for p := int32(0); p < d.Partitions; p++ {
		leaders[p] = liveIDs[int(p)%len(liveIDs)]
	}
	s.Topics[d.Topic] = &TopicInfo{Partitions: d.Partitions, Leaders: leaders}
	s.MetadataVersion++
	return nil
}

type assignLeaderData struct {
	Topic     string `msgpack:"topic"`
	Partition int32  `msgpack:"partition"`
	BrokerID  string `msgpack:"broker_id"`
}

func (s *ClusterState) applyAssignLeader(data []byte) error {
	var d assignLeaderData
	if err := msgpack.Unmarshal(data, &d); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.Topics[d.Topic]
	if !ok {
		return fmt.Errorf("unknown topic %q", d.Topic)
	}
	t.Leaders[d.Partition] = d.BrokerID
	s.MetadataVersion++
	return nil
}

type joinGroupData struct {
	GroupID   string   `msgpack:"group_id"`
	MemberID  string   `msgpack:"member_id"`
	Topics    []string `msgpack:"topics"`
	SessionMs int32    `msgpack:"session_ms"`
}

func (s *ClusterState) applyJoinGroup(data []byte) error {
	var d joinGroupData
	if err := msgpack.Unmarshal(data, &d); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	g, ok := s.Groups[d.GroupID]
	if !ok {
		g = &GroupState{Members: make(map[string]*ConsumerMember)}
		s.Groups[d.GroupID] = g
	}

	g.Generation++
	g.Members[d.MemberID] = &ConsumerMember{
		MemberID:   d.MemberID,
		Topics:     d.Topics,
		LastSeen:   time.Now(),
		SessionMs:  d.SessionMs,
		Generation: g.Generation,
	}
	s.MetadataVersion++
	return nil
}

type leaveGroupData struct {
	GroupID  string `msgpack:"group_id"`
	MemberID string `msgpack:"member_id"`
}

func (s *ClusterState) applyLeaveGroup(data []byte) error {
	var d leaveGroupData
	if err := msgpack.Unmarshal(data, &d); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	g, ok := s.Groups[d.GroupID]
	if !ok {
		return nil // already gone, idempotent
	}
	delete(g.Members, d.MemberID)
	g.Generation++
	s.MetadataVersion++
	return nil
}

var _ raft.StateMachine = (*ClusterState)(nil)
