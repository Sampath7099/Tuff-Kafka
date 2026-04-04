package proto

// --- Envelope ---

// Message is the top-level wire frame envelope.
type Message struct {
	Type    MessageType `msgpack:"t"`
	Payload interface{} `msgpack:"-"`
}

// --- Client ↔ Broker ---

type ProduceReq struct {
	Topic     string `msgpack:"topic"`
	Partition int32  `msgpack:"partition"`
	Key       string `msgpack:"key"`
	Value     []byte `msgpack:"value"`
}

type ProduceResp struct {
	Offset int64 `msgpack:"offset"`
}

type FetchReq struct {
	Topic     string `msgpack:"topic"`
	Partition int32  `msgpack:"partition"`
	Offset    int64  `msgpack:"offset"`
	MaxBytes  int32  `msgpack:"max_bytes"`
	MaxWaitMs int32  `msgpack:"max_wait_ms"` // 0 = no long-poll
}

type FetchResp struct {
	Messages []FetchedMessage `msgpack:"messages"`
}

type FetchedMessage struct {
	Offset int64  `msgpack:"offset"`
	Key    string `msgpack:"key"`
	Value  []byte `msgpack:"value"`
}

type CommitReq struct {
	Group     string `msgpack:"group"`
	Topic     string `msgpack:"topic"`
	Partition int32  `msgpack:"partition"`
	Offset    int64  `msgpack:"offset"`
}

// --- Client ↔ Controller ---

type CreateTopicReq struct {
	Topic      string `msgpack:"topic"`
	Partitions int32  `msgpack:"partitions"`
}

type MetadataReq struct {
	Topic string `msgpack:"topic"` // empty = all topics
}

type MetadataResp struct {
	Version int64                 `msgpack:"version"`
	Topics  map[string]TopicMeta  `msgpack:"topics"`
	Brokers map[string]BrokerInfo `msgpack:"brokers"`
}

type TopicMeta struct {
	Partitions int32            `msgpack:"partitions"`
	Leaders    map[int32]string `msgpack:"leaders"` // partition → brokerID
}

type BrokerInfo struct {
	ID   string `msgpack:"id"`
	Addr string `msgpack:"addr"`
}

type JoinGroupReq struct {
	GroupID   string   `msgpack:"group_id"`
	MemberID  string   `msgpack:"member_id"`
	Topics    []string `msgpack:"topics"`
	SessionMs int32    `msgpack:"session_ms"`
}

type JoinGroupResp struct {
	MemberID   string             `msgpack:"member_id"`
	Assignment map[string][]int32 `msgpack:"assignment"` // topic → partitions
	Generation int32              `msgpack:"generation"`
}

type HeartbeatReq struct {
	GroupID    string `msgpack:"group_id"`
	MemberID  string `msgpack:"member_id"`
	Generation int32 `msgpack:"generation"`
}

type HeartbeatResp struct {
	Rebalance bool `msgpack:"rebalance"`
}

type LeaveGroupReq struct {
	GroupID  string `msgpack:"group_id"`
	MemberID string `msgpack:"member_id"`
}

// --- Broker ↔ Controller ---

type BrokerHeartbeatReq struct {
	BrokerID string `msgpack:"broker_id"`
	Epoch    int64  `msgpack:"epoch"`
}

type BrokerRegisterReq struct {
	BrokerID string `msgpack:"broker_id"`
	Addr     string `msgpack:"addr"`
}

// --- Generic ---

type OKResp struct{}

type ErrorResp struct {
	Code    int32  `msgpack:"code"`
	Message string `msgpack:"message"`
}

func (e ErrorResp) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "protocol error"
}

// Error codes for ErrorResp.
const (
	ErrUnknownTopic        int32 = 1
	ErrNotLeader           int32 = 2
	ErrOffsetOutOfRange    int32 = 3
	ErrGroupNotFound       int32 = 4
	ErrRebalanceInProgress int32 = 5
	ErrNotController       int32 = 6
)
