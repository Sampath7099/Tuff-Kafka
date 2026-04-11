package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"github.com/Sampath7099/Tuff-Kafka/internal/broker"
	"gopkg.in/yaml.v3"
)

type nestedBrokerConfig struct {
	Broker struct {
		ID      string `yaml:"id"`
		Addr    string `yaml:"addr"`
		DataDir string `yaml:"data_dir"`
	} `yaml:"broker"`
	Controller struct {
		Addrs []string `yaml:"addrs"`
	} `yaml:"controller"`
	Heartbeat struct {
		IntervalMs     int32 `yaml:"interval_ms"`
		SessionTimeout int32 `yaml:"session_timeout_ms"`
	} `yaml:"heartbeat"`
	Fetch struct {
		MaxWaitMs int32 `yaml:"max_wait_ms"`
		MaxBytes  int32 `yaml:"max_bytes"`
	} `yaml:"fetch"`
}

func loadBrokerConfig(path string) (broker.Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return broker.Config{}, err
	}

	var flat broker.Config
	if err := yaml.Unmarshal(raw, &flat); err != nil {
		return broker.Config{}, err
	}

	var nested nestedBrokerConfig
	if err := yaml.Unmarshal(raw, &nested); err != nil {
		return broker.Config{}, err
	}

	// Prefer explicit flat schema when present; otherwise map nested schema.
	if flat.ID == "" && nested.Broker.ID != "" {
		flat.ID = nested.Broker.ID
	}
	if flat.Addr == "" && nested.Broker.Addr != "" {
		flat.Addr = nested.Broker.Addr
	}
	if flat.DataDir == "" && nested.Broker.DataDir != "" {
		flat.DataDir = nested.Broker.DataDir
	}
	if len(flat.ControllerAddrs) == 0 && len(nested.Controller.Addrs) > 0 {
		flat.ControllerAddrs = nested.Controller.Addrs
	}
	if flat.HeartbeatIntervalMs == 0 && nested.Heartbeat.IntervalMs > 0 {
		flat.HeartbeatIntervalMs = nested.Heartbeat.IntervalMs
	}
	if flat.SessionTimeoutMs == 0 && nested.Heartbeat.SessionTimeout > 0 {
		flat.SessionTimeoutMs = nested.Heartbeat.SessionTimeout
	}
	if flat.FetchMaxWaitMs == 0 && nested.Fetch.MaxWaitMs > 0 {
		flat.FetchMaxWaitMs = nested.Fetch.MaxWaitMs
	}
	if flat.FetchMaxBytes == 0 && nested.Fetch.MaxBytes > 0 {
		flat.FetchMaxBytes = nested.Fetch.MaxBytes
	}

	return flat, nil
}

func main() {
	cfgPath := flag.String("config", "configs/broker.yaml", "path to broker config file")
	flag.Parse()

	flatCfg, err := loadBrokerConfig(*cfgPath)
	if err != nil {
		log.Fatalf("parse config: %v", err)
	}
	if flatCfg.ID == "" || flatCfg.Addr == "" || flatCfg.DataDir == "" {
		log.Fatalf("invalid config: id, addr, and data_dir are required")
	}

	b, err := broker.NewBroker(flatCfg)
	if err != nil {
		log.Fatalf("new broker: %v", err)
	}

	if err := b.Register(); err != nil {
		log.Printf("broker Register warning: %v (will retry via heartbeat)", err)
	}

	interval := time.Duration(flatCfg.HeartbeatIntervalMs) * time.Millisecond
	if interval == 0 {
		interval = 3 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		b.RunHeartbeat(interval)
		_ = ctx
	}()

	log.Printf("broker %s listening on %s", flatCfg.ID, flatCfg.Addr)
	if err := b.Start(); err != nil {
		log.Fatalf("broker start: %v", err)
	}
}
