// Package proto defines the wire protocol for Tuff Cuffka.
package proto

// MessageType discriminates every kind of message on the wire.
type MessageType uint8

const (
	// Client ↔ Broker
	TypeProduce MessageType = 1
	TypeFetch   MessageType = 2
	TypeCommit  MessageType = 3

	// Client ↔ Controller
	TypeCreateTopic MessageType = 10
	TypeMetadata    MessageType = 11
	TypeJoinGroup   MessageType = 12
	TypeHeartbeat   MessageType = 13
	TypeLeaveGroup  MessageType = 14

	// Broker ↔ Controller
	TypeBrokerHeartbeat    MessageType = 20
	TypeBrokerRegister     MessageType = 21
	TypeReassignPartitions MessageType = 22
	TypeRebalanceTrigger   MessageType = 23

	// Raft internal
	TypeRaftRequestVote   MessageType = 30
	TypeRaftAppendEntries MessageType = 31
	TypeRaftInstallSnap   MessageType = 32

	// Generic
	TypeResponse MessageType = 50
	TypeError    MessageType = 51
)
