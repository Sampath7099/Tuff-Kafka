package broker

import (
	"time"

	"github.com/Sampath7099/Tuff-Kafka/internal/proto"
	"github.com/Sampath7099/Tuff-Kafka/internal/storage"
)

func (b *Broker) handleProduce(raw []byte) (interface{}, error) {
	var req proto.ProduceReq
	if err := proto.Decode(raw, &req); err != nil {
		return nil, err
	}

	if err := b.OpenLog(req.Topic, req.Partition); err != nil {
		return nil, err
	}

	b.mu.RLock()
	l, ok := b.logs[logKey{topic: req.Topic, partition: req.Partition}]
	b.mu.RUnlock()
	if !ok {
		return proto.ErrorResp{Code: proto.ErrUnknownTopic, Message: "unknown topic/partition"}, nil
	}

	offset, err := l.Append(req.Key, req.Value)
	if err != nil {
		return nil, err
	}

	b.notifyWaiters(logKey{topic: req.Topic, partition: req.Partition})
	return proto.ProduceResp{Offset: offset}, nil
}

func (b *Broker) handleFetch(raw []byte) (interface{}, error) {
	var req proto.FetchReq
	if err := proto.Decode(raw, &req); err != nil {
		return nil, err
	}

	if req.MaxWaitMs > 0 {
		_ = b.OpenLog(req.Topic, req.Partition)
	} else if err := b.OpenLog(req.Topic, req.Partition); err != nil {
		return nil, err
	}

	b.mu.RLock()
	l, ok := b.logs[logKey{topic: req.Topic, partition: req.Partition}]
	b.mu.RUnlock()
	if !ok {
		return proto.ErrorResp{Code: proto.ErrUnknownTopic, Message: "unknown topic/partition"}, nil
	}

	records, err := l.ReadBatch(req.Offset, int(req.MaxBytes))
	if err != nil {
		return nil, err
	}

	if len(records) == 0 && req.MaxWaitMs > 0 {
		records, err = b.waitForRecords(req.Topic, req.Partition, req.Offset, req.MaxBytes, req.MaxWaitMs)
		if err != nil {
			return nil, err
		}
	}

	resp := proto.FetchResp{Messages: make([]proto.FetchedMessage, 0, len(records))}
	for _, record := range records {
		resp.Messages = append(resp.Messages, proto.FetchedMessage{
			Offset: record.Offset,
			Key:    record.Key,
			Value:  record.Value,
		})
	}

	return resp, nil
}

func (b *Broker) handleCommit(raw []byte) (interface{}, error) {
	var req proto.CommitReq
	if err := proto.Decode(raw, &req); err != nil {
		return nil, err
	}

	if req.Offset < 0 {
		offset, err := b.offsets.Get(req.Group, req.Topic, req.Partition)
		if err != nil {
			return nil, err
		}
		return proto.ProduceResp{Offset: offset}, nil
	}

	return proto.OKResp{}, b.offsets.Commit(req.Group, req.Topic, req.Partition, req.Offset)
}

func (b *Broker) waitForRecords(topic string, partition int32, offset int64, maxBytes, maxWaitMs int32) ([]storage.LogRecord, error) {
	key := logKey{topic: topic, partition: partition}
	w := &fetchWaiter{
		offset:   offset,
		maxBytes: maxBytes,
		result:   make(chan fetchResult, 1),
	}

	b.mu.Lock()
	b.fetchWaiters[key] = append(b.fetchWaiters[key], w)
	b.mu.Unlock()

	t := time.NewTimer(time.Duration(maxWaitMs) * time.Millisecond)
	defer t.Stop()

	select {
	case res := <-w.result:
		return res.records, res.err
	case <-t.C:
		b.mu.Lock()
		waiters := b.fetchWaiters[key]
		for i := range waiters {
			if waiters[i] == w {
				waiters = append(waiters[:i], waiters[i+1:]...)
				break
			}
		}
		if len(waiters) == 0 {
			delete(b.fetchWaiters, key)
		} else {
			b.fetchWaiters[key] = waiters
		}
		b.mu.Unlock()
		return nil, nil
	}
}

// notifyWaiters wakes all blocked FETCH requests for a partition after a produce.
func (b *Broker) notifyWaiters(key logKey) {
	b.mu.Lock()
	waiters := b.fetchWaiters[key]
	delete(b.fetchWaiters, key)
	l := b.logs[key]
	b.mu.Unlock()

	if len(waiters) == 0 || l == nil {
		return
	}

	for _, w := range waiters {
		records, err := l.ReadBatch(w.offset, int(w.maxBytes))
		w.result <- fetchResult{records: records, err: err}
	}
}
