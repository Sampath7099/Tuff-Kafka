package proto

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestMetadataRoundtrip(t *testing.T) {
	resp := MetadataResp{
		Version: 4,
		Brokers: map[string]BrokerInfo{
			"broker-1": {ID: "broker-1", Addr: "127.0.0.1:1234"},
		},
		Topics: map[string]TopicMeta{
			"orders": {
				Partitions: 6,
				Leaders: map[int32]string{
					0: "broker-1", 1: "broker-2", 2: "broker-3",
					3: "broker-1", 4: "broker-2", 5: "broker-3",
				},
			},
		},
	}

	data, err := msgpack.Marshal(resp)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	t.Logf("Encoded %d bytes", len(data))

	var decoded MetadataResp
	if err := msgpack.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}

	t.Logf("Decoded: version=%d brokers=%d", decoded.Version, len(decoded.Brokers))
	for name, topic := range decoded.Topics {
		t.Logf("  topic=%s partitions=%d leaders=%v len_leaders=%d", name, topic.Partitions, topic.Leaders, len(topic.Leaders))
		for p := int32(0); p < 6; p++ {
			leader, ok := topic.Leaders[p]
			t.Logf("    partition %d -> leader=%q present=%v", p, leader, ok)
		}
	}

	// Verify all partitions have leaders
	topic := decoded.Topics["orders"]
	for p := int32(0); p < 6; p++ {
		leader, ok := topic.Leaders[p]
		if !ok || leader == "" {
			t.Errorf("partition %d has no leader (ok=%v leader=%q)", p, ok, leader)
		}
	}
}
