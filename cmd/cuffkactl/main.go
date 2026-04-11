// Admin CLI for Tuff Cuffka. Subcommands: create-topic, describe, produce, consume.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Sampath7099/Tuff-Kafka/internal/consumer"
	"github.com/Sampath7099/Tuff-Kafka/internal/producer"
	"github.com/Sampath7099/Tuff-Kafka/internal/proto"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "create-topic":
		err = runCreateTopic(os.Args[2:])
	case "describe", "describe-cluster":
		err = runDescribe(os.Args[2:])
	case "produce":
		err = runProduce(os.Args[2:])
	case "consume":
		err = runConsume(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  cuffkactl create-topic --controllers addr1,addr2 --topic orders --partitions 6")
	fmt.Fprintln(os.Stderr, "  cuffkactl describe --controllers addr1,addr2 [--topic orders]")
	fmt.Fprintln(os.Stderr, "  cuffkactl produce --controllers addr1,addr2 --topic orders [--key k1] [--value text]")
	fmt.Fprintln(os.Stderr, "  cuffkactl consume --controllers addr1,addr2 --topic orders --group g1 [--count 10]")
}

func parseControllers(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func requestAnyController(ctrlAddrs []string, msgType proto.MessageType, payload interface{}, out interface{}) error {
	var lastErr error
	for _, addr := range ctrlAddrs {
		conn, err := net.DialTimeout("tcp", addr, 1500*time.Millisecond)
		if err != nil {
			lastErr = err
			continue
		}

		_ = proto.WriteFrame(conn, msgType, payload)
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		rType, raw, err := proto.ReadFrame(conn)
		_ = conn.Close()
		if err != nil {
			lastErr = err
			continue
		}

		if rType == proto.TypeError {
			var e proto.ErrorResp
			if derr := proto.Decode(raw, &e); derr != nil {
				lastErr = derr
				continue
			}
			// Non-leader / transient controller errors: try next controller.
			if e.Code == proto.ErrNotController {
				lastErr = e
				continue
			}
			return e
		}

		if out != nil {
			if err := proto.Decode(raw, out); err != nil {
				lastErr = err
				continue
			}
		}
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no controller reachable")
	}
	return lastErr
}

func runCreateTopic(args []string) error {
	fs := flag.NewFlagSet("create-topic", flag.ContinueOnError)
	controllers := fs.String("controllers", "127.0.0.1:9093", "comma-separated controller addresses")
	topic := fs.String("topic", "", "topic name")
	partitions := fs.Int("partitions", 1, "partition count")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" {
		return fmt.Errorf("--topic is required")
	}
	if *partitions <= 0 {
		return fmt.Errorf("--partitions must be > 0")
	}

	ctrls := parseControllers(*controllers)
	return requestAnyController(ctrls, proto.TypeCreateTopic, proto.CreateTopicReq{
		Topic:      *topic,
		Partitions: int32(*partitions),
	}, &proto.OKResp{})
}

func runDescribe(args []string) error {
	fs := flag.NewFlagSet("describe", flag.ContinueOnError)
	controllers := fs.String("controllers", "127.0.0.1:9093", "comma-separated controller addresses")
	topic := fs.String("topic", "", "optional topic name")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctrls := parseControllers(*controllers)
	var resp proto.MetadataResp
	if err := requestAnyController(ctrls, proto.TypeMetadata, proto.MetadataReq{Topic: *topic}, &resp); err != nil {
		return err
	}

	fmt.Printf("version=%d brokers=%d topics=%d\n", resp.Version, len(resp.Brokers), len(resp.Topics))

	brokerIDs := make([]string, 0, len(resp.Brokers))
	for id := range resp.Brokers {
		brokerIDs = append(brokerIDs, id)
	}
	sort.Strings(brokerIDs)
	for _, id := range brokerIDs {
		b := resp.Brokers[id]
		fmt.Printf("broker %s -> %s\n", b.ID, b.Addr)
	}

	topics := make([]string, 0, len(resp.Topics))
	for t := range resp.Topics {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	for _, t := range topics {
		tm := resp.Topics[t]
		fmt.Printf("topic %s partitions=%d\n", t, tm.Partitions)
		parts := make([]int, 0, len(tm.Leaders))
		for p := range tm.Leaders {
			parts = append(parts, int(p))
		}
		sort.Ints(parts)
		for _, p := range parts {
			fmt.Printf("  p%d -> %s\n", p, tm.Leaders[int32(p)])
		}
	}

	return nil
}

func runProduce(args []string) error {
	fs := flag.NewFlagSet("produce", flag.ContinueOnError)
	controllers := fs.String("controllers", "127.0.0.1:9093", "comma-separated controller addresses")
	topic := fs.String("topic", "", "topic name")
	key := fs.String("key", "", "message key (optional)")
	value := fs.String("value", "", "single message value; if empty, read lines from stdin")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" {
		return fmt.Errorf("--topic is required")
	}

	p := producer.NewProducer(parseControllers(*controllers))
	defer p.Close()

	if *value != "" {
		off, err := p.Produce(*topic, *key, []byte(*value))
		if err != nil {
			return err
		}
		fmt.Printf("offset=%d\n", off)
		return nil
	}

	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		line := sc.Text()
		off, err := p.Produce(*topic, *key, []byte(line))
		if err != nil {
			return err
		}
		fmt.Printf("offset=%d\n", off)
	}
	return sc.Err()
}

func runConsume(args []string) error {
	fs := flag.NewFlagSet("consume", flag.ContinueOnError)
	controllers := fs.String("controllers", "127.0.0.1:9093", "comma-separated controller addresses")
	topic := fs.String("topic", "", "topic name")
	group := fs.String("group", "", "consumer group id")
	count := fs.Int("count", 10, "number of records to print (0 = run forever)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" || *group == "" {
		return fmt.Errorf("--topic and --group are required")
	}

	c := consumer.NewConsumer(parseControllers(*controllers), *group)
	c.Subscribe(*topic)
	g := consumer.NewConsumerGroup(c)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := g.Join(ctx); err != nil {
		return err
	}

	printed := 0
	for *count == 0 || printed < *count {
		pollCtx, pollCancel := context.WithTimeout(context.Background(), 2*time.Second)
		recs, err := c.Poll(pollCtx, 100)
		pollCancel()
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		for _, r := range recs {
			fmt.Printf("topic=%s partition=%d offset=%d key=%q value=%s\n", r.Topic, r.Partition, r.Offset, r.Key, string(r.Value))
			printed++
			if *count > 0 && printed >= *count {
				break
			}
		}
	}

	_ = c.CommitAll()
	return nil
}
