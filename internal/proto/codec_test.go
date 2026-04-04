package proto

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCodec_WriteReadRoundTrip_ProduceReq verifies that a ProduceReq survives
// encode → write → read → decode without data loss.
func TestCodec_WriteReadRoundTrip_ProduceReq(t *testing.T) {
	var buf bytes.Buffer
	original := ProduceReq{
		Topic:     "orders",
		Partition: 3,
		Key:       "user-42",
		Value:     []byte("order-data-payload"),
	}

	err := WriteFrame(&buf, TypeProduce, original)
	require.NoError(t, err, "WriteFrame should succeed")

	msgType, raw, err := ReadFrame(&buf)
	require.NoError(t, err, "ReadFrame should succeed")
	assert.Equal(t, TypeProduce, msgType, "message type should be TypeProduce")

	var decoded ProduceReq
	err = Decode(raw, &decoded)
	require.NoError(t, err, "Decode should succeed")
	assert.Equal(t, original.Topic, decoded.Topic)
	assert.Equal(t, original.Partition, decoded.Partition)
	assert.Equal(t, original.Key, decoded.Key)
	assert.Equal(t, original.Value, decoded.Value)
}

// TestCodec_WriteReadRoundTrip_FetchReq verifies FetchReq round-trip.
func TestCodec_WriteReadRoundTrip_FetchReq(t *testing.T) {
	var buf bytes.Buffer
	original := FetchReq{
		Topic:     "events",
		Partition: 7,
		Offset:    12345,
		MaxBytes:  1048576,
		MaxWaitMs: 500,
	}

	err := WriteFrame(&buf, TypeFetch, original)
	require.NoError(t, err)

	msgType, raw, err := ReadFrame(&buf)
	require.NoError(t, err)
	assert.Equal(t, TypeFetch, msgType)

	var decoded FetchReq
	err = Decode(raw, &decoded)
	require.NoError(t, err)
	assert.Equal(t, original.Topic, decoded.Topic)
	assert.Equal(t, original.Partition, decoded.Partition)
	assert.Equal(t, original.Offset, decoded.Offset)
	assert.Equal(t, original.MaxBytes, decoded.MaxBytes)
	assert.Equal(t, original.MaxWaitMs, decoded.MaxWaitMs)
}

// TestCodec_WriteReadRoundTrip_AllMessageTypes is a table-driven test that
// verifies every request/response type survives a full round-trip through
// WriteFrame → ReadFrame → Decode.
func TestCodec_WriteReadRoundTrip_AllMessageTypes(t *testing.T) {
	cases := []struct {
		name    string
		msgType MessageType
		payload interface{}
		decode  func([]byte) error // custom decoder to verify
	}{
		{
			name:    "ProduceReq",
			msgType: TypeProduce,
			payload: ProduceReq{Topic: "t1", Partition: 0, Key: "k", Value: []byte("v")},
			decode: func(raw []byte) error {
				var d ProduceReq
				return Decode(raw, &d)
			},
		},
		{
			name:    "ProduceResp",
			msgType: TypeResponse,
			payload: ProduceResp{Offset: 42},
			decode: func(raw []byte) error {
				var d ProduceResp
				return Decode(raw, &d)
			},
		},
		{
			name:    "FetchReq",
			msgType: TypeFetch,
			payload: FetchReq{Topic: "t1", Partition: 1, Offset: 100, MaxBytes: 4096, MaxWaitMs: 200},
			decode: func(raw []byte) error {
				var d FetchReq
				return Decode(raw, &d)
			},
		},
		{
			name:    "FetchResp",
			msgType: TypeResponse,
			payload: FetchResp{Messages: []FetchedMessage{{Offset: 0, Key: "k1", Value: []byte("v1")}}},
			decode: func(raw []byte) error {
				var d FetchResp
				return Decode(raw, &d)
			},
		},
		{
			name:    "CommitReq",
			msgType: TypeCommit,
			payload: CommitReq{Group: "g1", Topic: "t1", Partition: 2, Offset: 99},
			decode: func(raw []byte) error {
				var d CommitReq
				return Decode(raw, &d)
			},
		},
		{
			name:    "CreateTopicReq",
			msgType: TypeCreateTopic,
			payload: CreateTopicReq{Topic: "new-topic", Partitions: 6},
			decode: func(raw []byte) error {
				var d CreateTopicReq
				return Decode(raw, &d)
			},
		},
		{
			name:    "MetadataReq",
			msgType: TypeMetadata,
			payload: MetadataReq{Topic: "orders"},
			decode: func(raw []byte) error {
				var d MetadataReq
				return Decode(raw, &d)
			},
		},
		{
			name:    "MetadataResp",
			msgType: TypeResponse,
			payload: MetadataResp{
				Version: 5,
				Topics:  map[string]TopicMeta{"t1": {Partitions: 3, Leaders: map[int32]string{0: "b1", 1: "b2", 2: "b3"}}},
				Brokers: map[string]BrokerInfo{"b1": {ID: "b1", Addr: "localhost:9092"}},
			},
			decode: func(raw []byte) error {
				var d MetadataResp
				return Decode(raw, &d)
			},
		},
		{
			name:    "JoinGroupReq",
			msgType: TypeJoinGroup,
			payload: JoinGroupReq{GroupID: "g1", MemberID: "m1", Topics: []string{"t1", "t2"}, SessionMs: 10000},
			decode: func(raw []byte) error {
				var d JoinGroupReq
				return Decode(raw, &d)
			},
		},
		{
			name:    "JoinGroupResp",
			msgType: TypeResponse,
			payload: JoinGroupResp{MemberID: "m1", Assignment: map[string][]int32{"t1": {0, 1}}, Generation: 3},
			decode: func(raw []byte) error {
				var d JoinGroupResp
				return Decode(raw, &d)
			},
		},
		{
			name:    "HeartbeatReq",
			msgType: TypeHeartbeat,
			payload: HeartbeatReq{GroupID: "g1", MemberID: "m1", Generation: 3},
			decode: func(raw []byte) error {
				var d HeartbeatReq
				return Decode(raw, &d)
			},
		},
		{
			name:    "HeartbeatResp",
			msgType: TypeResponse,
			payload: HeartbeatResp{Rebalance: true},
			decode: func(raw []byte) error {
				var d HeartbeatResp
				return Decode(raw, &d)
			},
		},
		{
			name:    "LeaveGroupReq",
			msgType: TypeLeaveGroup,
			payload: LeaveGroupReq{GroupID: "g1", MemberID: "m1"},
			decode: func(raw []byte) error {
				var d LeaveGroupReq
				return Decode(raw, &d)
			},
		},
		{
			name:    "BrokerHeartbeatReq",
			msgType: TypeBrokerHeartbeat,
			payload: BrokerHeartbeatReq{BrokerID: "broker-1", Epoch: 7},
			decode: func(raw []byte) error {
				var d BrokerHeartbeatReq
				return Decode(raw, &d)
			},
		},
		{
			name:    "BrokerRegisterReq",
			msgType: TypeBrokerRegister,
			payload: BrokerRegisterReq{BrokerID: "broker-1", Addr: "10.0.0.1:9092"},
			decode: func(raw []byte) error {
				var d BrokerRegisterReq
				return Decode(raw, &d)
			},
		},
		{
			name:    "OKResp",
			msgType: TypeResponse,
			payload: OKResp{},
			decode: func(raw []byte) error {
				var d OKResp
				return Decode(raw, &d)
			},
		},
		{
			name:    "ErrorResp",
			msgType: TypeError,
			payload: ErrorResp{Code: ErrNotLeader, Message: "not the partition leader"},
			decode: func(raw []byte) error {
				var d ErrorResp
				return Decode(raw, &d)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := WriteFrame(&buf, tc.msgType, tc.payload)
			require.NoError(t, err, "WriteFrame should succeed for %s", tc.name)

			msgType, raw, err := ReadFrame(&buf)
			require.NoError(t, err, "ReadFrame should succeed for %s", tc.name)
			assert.Equal(t, tc.msgType, msgType, "message type mismatch for %s", tc.name)

			err = tc.decode(raw)
			require.NoError(t, err, "Decode should succeed for %s", tc.name)
		})
	}
}

// TestCodec_LargePayload verifies that payloads near maxFrameSize (15 MB)
// are correctly encoded and decoded without error.
func TestCodec_LargePayload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large payload test in short mode")
	}

	largeValue := make([]byte, 15*1024*1024) // 15 MB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	var buf bytes.Buffer
	original := ProduceReq{
		Topic:     "big-topic",
		Partition: 0,
		Key:       "big-key",
		Value:     largeValue,
	}

	err := WriteFrame(&buf, TypeProduce, original)
	require.NoError(t, err, "WriteFrame should handle large payload")

	msgType, raw, err := ReadFrame(&buf)
	require.NoError(t, err, "ReadFrame should handle large payload")
	assert.Equal(t, TypeProduce, msgType)

	var decoded ProduceReq
	err = Decode(raw, &decoded)
	require.NoError(t, err)
	assert.Equal(t, len(largeValue), len(decoded.Value), "large payload size should match")
	assert.Equal(t, largeValue[:100], decoded.Value[:100], "first 100 bytes should match")
	assert.Equal(t, largeValue[len(largeValue)-100:], decoded.Value[len(decoded.Value)-100:], "last 100 bytes should match")
}

// TestCodec_FrameTooLarge verifies that ReadFrame returns an error (not a panic)
// when encountering a frame with a length prefix exceeding maxFrameSize.
func TestCodec_FrameTooLarge(t *testing.T) {
	// Craft a buffer with a length prefix indicating 20 MB (> 16 MB limit)
	var buf bytes.Buffer
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 20*1024*1024)
	buf.Write(lenBuf[:])
	// Don't need to write actual payload — ReadFrame should reject based on length

	_, _, err := ReadFrame(&buf)
	require.Error(t, err, "ReadFrame should reject frame exceeding maxFrameSize")
	assert.Contains(t, err.Error(), "frame too large", "error should mention frame too large")
}

// TestCodec_ShortRead verifies that ReadFrame properly propagates
// io.ErrUnexpectedEOF when the reader provides fewer bytes than the
// length prefix indicates.
func TestCodec_ShortRead(t *testing.T) {
	var buf bytes.Buffer

	// Write a length prefix indicating 100 bytes, but only provide 10
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 100)
	buf.Write(lenBuf[:])
	buf.Write(make([]byte, 10)) // only 10 bytes instead of 100

	_, _, err := ReadFrame(&buf)
	require.Error(t, err, "ReadFrame should fail on short read")
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF, "error should be io.ErrUnexpectedEOF")
}

// TestCodec_EmptyFrame verifies that ReadFrame returns an error when
// the frame body is zero bytes (no type byte, no payload).
func TestCodec_EmptyFrame(t *testing.T) {
	var buf bytes.Buffer

	// Write a length prefix of 0
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 0)
	buf.Write(lenBuf[:])

	_, _, err := ReadFrame(&buf)
	require.Error(t, err, "ReadFrame should reject empty frame")
	assert.Contains(t, err.Error(), "empty frame", "error should mention empty frame")
}

// TestCodec_ConcurrentWrites verifies that WriteFrame is safe to call from
// multiple goroutines when writes are serialized by the caller (e.g., via mutex).
// This test ensures no panics or data corruption occur.
func TestCodec_ConcurrentWrites(t *testing.T) {
	var mu sync.Mutex
	var buf bytes.Buffer
	const numWriters = 50

	var wg sync.WaitGroup
	wg.Add(numWriters)

	for i := 0; i < numWriters; i++ {
		go func(id int) {
			defer wg.Done()
			msg := ProduceReq{
				Topic:     "concurrent-topic",
				Partition: int32(id % 4),
				Key:       "key",
				Value:     []byte("value"),
			}
			mu.Lock()
			err := WriteFrame(&buf, TypeProduce, msg)
			mu.Unlock()
			assert.NoError(t, err, "concurrent WriteFrame should succeed")
		}(i)
	}
	wg.Wait()

	// Read all frames back — should get exactly numWriters frames
	framesRead := 0
	reader := bytes.NewReader(buf.Bytes())
	for {
		msgType, raw, err := ReadFrame(reader)
		if err != nil {
			break
		}
		assert.Equal(t, TypeProduce, msgType)
		var decoded ProduceReq
		err = Decode(raw, &decoded)
		assert.NoError(t, err)
		assert.Equal(t, "concurrent-topic", decoded.Topic)
		framesRead++
	}
	assert.Equal(t, numWriters, framesRead, "should read back exactly %d frames", numWriters)
}

// TestCodec_ZeroLengthValue verifies that a ProduceReq with an empty value
// (nil or zero-length byte slice) round-trips correctly.
func TestCodec_ZeroLengthValue(t *testing.T) {
	cases := []struct {
		name  string
		value []byte
	}{
		{"nil value", nil},
		{"empty slice", []byte{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			original := ProduceReq{
				Topic:     "t1",
				Partition: 0,
				Key:       "k1",
				Value:     tc.value,
			}

			err := WriteFrame(&buf, TypeProduce, original)
			require.NoError(t, err)

			_, raw, err := ReadFrame(&buf)
			require.NoError(t, err)

			var decoded ProduceReq
			err = Decode(raw, &decoded)
			require.NoError(t, err)
			assert.Equal(t, original.Key, decoded.Key)
			// Both nil and empty slice should decode to a value with length 0
			assert.Equal(t, 0, len(decoded.Value), "decoded value should have zero length")
		})
	}
}

// TestCodec_MessageTypePreserved verifies that each MessageType constant
// is correctly preserved through the frame encoding. The type byte should
// exactly match the original MessageType after round-trip.
func TestCodec_MessageTypePreserved(t *testing.T) {
	types := []MessageType{
		TypeProduce, TypeFetch, TypeCommit,
		TypeCreateTopic, TypeMetadata, TypeJoinGroup, TypeHeartbeat, TypeLeaveGroup,
		TypeBrokerHeartbeat, TypeBrokerRegister,
		TypeReassignPartitions, TypeRebalanceTrigger,
		TypeRaftRequestVote, TypeRaftAppendEntries, TypeRaftInstallSnap,
		TypeResponse, TypeError,
	}

	for _, mt := range types {
		t.Run(string(rune('0'+mt)), func(t *testing.T) {
			var buf bytes.Buffer
			// Use a simple OKResp as payload — we only care about the type byte
			err := WriteFrame(&buf, mt, OKResp{})
			require.NoError(t, err)

			decoded, _, err := ReadFrame(&buf)
			require.NoError(t, err)
			assert.Equal(t, mt, decoded, "MessageType %d should be preserved", mt)
		})
	}
}
