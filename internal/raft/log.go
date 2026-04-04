package raft

import (
	"github.com/vmihailenco/msgpack/v5"
)

// Entry is a single command in the Raft replicated log.
type Entry struct {
	Term    int64  `msgpack:"term"`
	Index   int64  `msgpack:"index"`
	Payload []byte `msgpack:"payload"` // msgpack-encoded Command
}

// CommandType discriminates metadata mutations applied to the state machine.
type CommandType uint8

const (
	CmdRegisterBroker    CommandType = 1
	CmdDeregisterBroker  CommandType = 2
	CmdCreateTopic       CommandType = 3
	CmdAssignLeader      CommandType = 4
	CmdJoinGroup         CommandType = 5
	CmdLeaveGroup        CommandType = 6
	CmdCommitGroupOffset CommandType = 7
)

// Command wraps a CommandType and its serialized data.
type Command struct {
	Type CommandType `msgpack:"type"`
	Data []byte      `msgpack:"data"`
}

// RaftLog is the in-memory Raft log. 1-based per the Raft paper;
// entries[0] is a dummy sentinel (index=0, term=0).
type RaftLog struct {
	entries []Entry
}

// NewRaftLog creates a log with a dummy sentinel at index 0.
func NewRaftLog() *RaftLog {
	return &RaftLog{
		entries: []Entry{
			{
				Term:    0,
				Index:   0,
				Payload: nil,
			},
		},
	}
}

func (l *RaftLog) LastIndex() int64 {
	// returns the index of the last entry in the log
	return int64(len(l.entries) - 1)
}

func (l *RaftLog) LastTerm() int64 {
	// returns the term of the last entry in the log
	return l.entries[l.LastIndex()].Term
}

func (l *RaftLog) Entry(index int64) Entry {
	// returns entry at `index` (inclusive)
	if index < 0 || index > l.LastIndex() {
		return Entry{}
	}
	return l.entries[index]
}

func (l *RaftLog) Append(e Entry) {
	// appends a single entry to the end of the log
	l.entries = append(l.entries, e)
}

func (l *RaftLog) TruncateFrom(index int64) {
	// removes entries from index `index` (inclusive) to the end
	l.entries = l.entries[:index]
}

func (l *RaftLog) Entries(from, to int64) []Entry {
	// returns entries from index `from` (inclusive) to `to` (exclusive)
	return l.entries[from:to]
}

// EncodeCommand serializes a Command to msgpack.
func EncodeCommand(cmd Command) ([]byte, error) {
	return msgpack.Marshal(cmd)
}

// DecodeCommand deserializes msgpack bytes into a Command.
func DecodeCommand(raw []byte) (Command, error) {
	var cmd Command
	err := msgpack.Unmarshal(raw, &cmd)
	return cmd, err
}
