// Controller entry point. Starts the Raft node and controller server.
package main

import (
	"flag"
	"log"
	"os"

	"github.com/Sampath7099/Tuff-Kafka/internal/controller"
	"github.com/Sampath7099/Tuff-Kafka/internal/raft"
	"gopkg.in/yaml.v3"
)

type nestedControllerConfig struct {
	Controller struct {
		ID      string `yaml:"id"`
		Addr    string `yaml:"addr"`
		RaftDir string `yaml:"raft_dir"`
	} `yaml:"controller"`
	Raft struct {
		Peers []controller.PeerConfig `yaml:"peers"`
	} `yaml:"raft"`
	BrokerSessionTimeoutMs   int32 `yaml:"broker_session_timeout_ms"`
	ConsumerSessionTimeoutMs int32 `yaml:"consumer_session_timeout_ms"`
}

func loadControllerConfig(path string) (controller.Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return controller.Config{}, err
	}

	var flat controller.Config
	if err := yaml.Unmarshal(raw, &flat); err != nil {
		return controller.Config{}, err
	}

	var nested nestedControllerConfig
	if err := yaml.Unmarshal(raw, &nested); err != nil {
		return controller.Config{}, err
	}

	// Prefer explicit flat schema when present; otherwise map nested schema.
	if flat.ID == "" && nested.Controller.ID != "" {
		flat.ID = nested.Controller.ID
	}
	if flat.Addr == "" && nested.Controller.Addr != "" {
		flat.Addr = nested.Controller.Addr
	}
	if flat.RaftDir == "" && nested.Controller.RaftDir != "" {
		flat.RaftDir = nested.Controller.RaftDir
	}
	if len(flat.Peers) == 0 && len(nested.Raft.Peers) > 0 {
		flat.Peers = nested.Raft.Peers
	}
	if flat.BrokerSessionTimeoutMs == 0 && nested.BrokerSessionTimeoutMs > 0 {
		flat.BrokerSessionTimeoutMs = nested.BrokerSessionTimeoutMs
	}
	if flat.ConsumerSessionTimeoutMs == 0 && nested.ConsumerSessionTimeoutMs > 0 {
		flat.ConsumerSessionTimeoutMs = nested.ConsumerSessionTimeoutMs
	}

	return flat, nil
}

func main() {
	cfgPath := flag.String("config", "configs/controller.yaml", "path to controller config file")
	flag.Parse()

	// 1. Load Config from YAML.
	cfg, err := loadControllerConfig(*cfgPath)
	if err != nil {
		log.Fatalf("parse config: %v", err)
	}
	if cfg.ID == "" || cfg.Addr == "" {
		log.Fatalf("invalid config: id and addr are required")
	}

	// 2. Build peer maps for Raft transport.
	peerAddrs := make(map[string]string, len(cfg.Peers))
	peerIDs := make([]string, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		peerAddrs[p.ID] = p.Addr
		peerIDs = append(peerIDs, p.ID)
	}

	// 3. Create state machine.
	state := controller.NewClusterState()

	// 4. Create Raft transport and node.
	transport := raft.NewTCPTransport(peerAddrs)
	raftNode := raft.NewRaftNode(cfg.ID, peerIDs, state, transport)

	// 5. Start Raft.
	raftNode.Start()
	log.Printf("raft node %s started, peers: %v", cfg.ID, peerIDs)

	// 6. Start controller server (blocks until error).
	srv := controller.NewServer(cfg, state, raftNode)
	log.Printf("controller server listening on %s", cfg.Addr)
	if err := srv.Start(); err != nil {
		log.Fatalf("controller server: %v", err)
	}
}
