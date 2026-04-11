package raft

import (
	"fmt"
	"net"

	"github.com/Sampath7099/Tuff-Kafka/internal/proto"
	"github.com/vmihailenco/msgpack/v5"
)

// TCPTransport implements Transport over TCP using proto's framing.
type TCPTransport struct {
	peers map[string]string // peerID → address
}

func NewTCPTransport(peers map[string]string) *TCPTransport {
	p := make(map[string]string, len(peers))
	for k, v := range peers {
		p[k] = v
	}
	return &TCPTransport{peers: p}
}

func (t *TCPTransport) SendRequestVote(peerID string, req RequestVoteReq) (RequestVoteResp, error) {
	addr, ok := t.peers[peerID]
	if !ok {
		return RequestVoteResp{}, fmt.Errorf("unknown peer %q", peerID)
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return RequestVoteResp{}, err
	}
	defer conn.Close()

	if err := proto.WriteFrame(conn, proto.TypeRaftRequestVote, req); err != nil {
		return RequestVoteResp{}, err
	}
	_, raw, err := proto.ReadFrame(conn)
	if err != nil {
		return RequestVoteResp{}, err
	}
	var resp RequestVoteResp
	if err := msgpack.Unmarshal(raw, &resp); err != nil {
		return RequestVoteResp{}, err
	}
	return resp, nil
}

func (t *TCPTransport) SendAppendEntries(peerID string, req AppendEntriesReq) (AppendEntriesResp, error) {
	addr, ok := t.peers[peerID]
	if !ok {
		return AppendEntriesResp{}, fmt.Errorf("unknown peer %q", peerID)
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return AppendEntriesResp{}, err
	}
	defer conn.Close()

	if err := proto.WriteFrame(conn, proto.TypeRaftAppendEntries, req); err != nil {
		return AppendEntriesResp{}, err
	}
	_, raw, err := proto.ReadFrame(conn)
	if err != nil {
		return AppendEntriesResp{}, err
	}
	var resp AppendEntriesResp
	if err := msgpack.Unmarshal(raw, &resp); err != nil {
		return AppendEntriesResp{}, err
	}
	return resp, nil
}
