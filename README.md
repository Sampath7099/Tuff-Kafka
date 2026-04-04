# Tuff Cuffka

A ground-up Kafka reimplementation in Go. Distributed pub-sub message broker with Raft-based controller quorum (KRaft-inspired), append-only partition logs, and consumer group coordination.

## Architecture

```
┌────────────┐         ┌────────────────┐         ┌────────────┐
│  Producer   │────────▶│     Broker     │◀────────│  Consumer   │
│  (client)   │ PRODUCE │  (data plane)  │ FETCH   │  (client)   │
└────────────┘         └───────┬────────┘         └──────┬─────┘
                               │ heartbeat               │ group
                               ▼                         ▼
                       ┌────────────────────────────────────┐
                       │     Controller Quorum (3 nodes)    │
                       │         Raft consensus             │
                       │     metadata · topics · groups     │
                       └────────────────────────────────────┘
```

**Broker** — owns partition logs, serves PRODUCE/FETCH/COMMIT over TCP.  
**Controller** — Raft-replicated metadata store (topics, brokers, consumer groups).  
**Wire protocol** — length-prefixed MessagePack frames over TCP.

## Project Structure

```
cuffka/
├── cmd/                    # Binary entry points
│   ├── broker/main.go
│   ├── controller/main.go
│   └── cuffkactl/main.go   # Admin CLI
│
├── configs/                # Example YAML configurations
│   ├── broker.yaml.example
│   └── controller.yaml.example
│
├── docs/                   # Design documents
│   └── initial_guide.md
│
├── internal/               # All library code (Go visibility enforced)
│   ├── proto/              # Wire protocol: types, messages, codec
│   ├── storage/            # PartitionLog + OffsetStore
│   ├── raft/               # Raft consensus engine
│   ├── controller/         # Controller server + state machine
│   ├── broker/             # Broker server + handlers
│   ├── producer/           # Producer client library
│   ├── consumer/           # Consumer + ConsumerGroup client
│   └── testenv/            # Integration test harness
│
├── tests/integration/      # End-to-end tests (build-tagged)
├── go.mod
├── go.sum
└── Makefile
```

> **Note:** Go requires test files (`_test.go`) to be colocated with source files. The `internal/` directory is enforced by the Go compiler — external packages cannot import from it. This is idiomatic Go project layout.

## Build & Test

```bash
make build              # Compile all binaries
make test               # Unit tests with -race
make test-verbose       # Unit tests with -v
make test-integration   # Integration tests (requires full cluster)
make lint               # go vet
make clean              # Clear caches
```

## Admin CLI (`cuffkactl`)

`cuffkactl` provides simple admin and client operations against the controller quorum.

```bash
# Create a topic
go run ./cmd/cuffkactl create-topic \
        --controllers 127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095 \
        --topic orders \
        --partitions 6

# Describe cluster (or one topic with --topic)
go run ./cmd/cuffkactl describe \
        --controllers 127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095

# Produce one record
go run ./cmd/cuffkactl produce \
        --controllers 127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095 \
        --topic orders \
        --key user-42 \
        --value "hello"

# Consume 10 records
go run ./cmd/cuffkactl consume \
        --controllers 127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095 \
        --topic orders \
        --group g1 \
        --count 10
```

## Dependencies

| Package | Purpose |
|---------|---------|
| `vmihailenco/msgpack/v5` | Wire protocol serialization |
| `stretchr/testify` | Test assertions |
| `gopkg.in/yaml.v3` | Configuration parsing |

## Implementation Phases

| # | Package | Description |
|---|---------|-------------|
| 1 | `proto` | Wire protocol codec |
| 2 | `storage` | Append-only log + offset store |
| 3 | `raft` | Raft consensus engine |
| 4 | `controller` | Metadata state machine + server |
| 5 | `broker` | Data plane server |
| 6 | `producer` | Producer client |
| 7 | `consumer` | Consumer + group coordinator |
| 8-9 | `testenv` + `tests/` | Integration tests |

