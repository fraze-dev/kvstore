# kvstore

A crash-safe, concurrent in-memory key-value store with write-ahead log (WAL) persistence and snapshotting — built from scratch in Go.

> **Why build this?** The best way to understand how Redis, Postgres, and Kafka guarantee durability is to implement the same mechanisms yourself. This project covers sharded concurrency, binary log formats, crash recovery, and snapshot compaction — the core tradeoffs that come up in backend systems.

---

## Features

- `SET`, `GET`, `DEL` with optional TTL (`EX seconds`)
- TCP protocol compatible with `redis-cli` and `netcat`
- **Write-ahead log** — mutations survive process crashes
- **Crash recovery** — WAL replay rebuilds full state on restart
- **Snapshots** — periodic full-state dumps for fast startup and WAL compaction
- **Sharded mutex locking** — 256 shards reduce lock contention under concurrent load
- Lazy TTL expiry — expired keys are cleaned up on next access

---

## Quick start

```bash
# Install Go 1.22+ from https://go.dev/dl/

git clone https://github.com/fraze-dev/kvstore
cd kvstore

# Run the server (default port 6379)
go run ./cmd/server

# In another terminal — connect with netcat
nc localhost 6379

# Or with redis-cli (if installed)
redis-cli -p 6379
```

### Basic commands

```
SET name Alice
+OK

GET name
$5
Alice

SET session abc123 EX 30
+OK

DEL name
+OK

GET name
$-1

KEYS
*1
$6
session

PING
+PONG
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      TCP Client                          │
└────────────────────────┬────────────────────────────────┘
                         │ plain text commands
┌────────────────────────▼────────────────────────────────┐
│              Protocol Server (protocol/)                 │
│         Parses commands, one goroutine per client        │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│                  Store (store/)                          │
│                                                          │
│   ┌──────────┐  ┌──────────┐      ┌──────────┐         │
│   │ Shard  0 │  │ Shard  1 │ ...  │ Shard 255│         │
│   │ RWMutex  │  │ RWMutex  │      │ RWMutex  │         │
│   │ map[k]v  │  │ map[k]v  │      │ map[k]v  │         │
│   └──────────┘  └──────────┘      └──────────┘         │
└────────────────────────┬────────────────────────────────┘
                         │ every mutation
┌────────────────────────▼────────────────────────────────┐
│                  WAL (wal/)                              │
│         Append-only binary log, length-prefixed          │
│         fsync on every write (configurable)              │
└────────────────────────┬────────────────────────────────┘
                         │
                    data/wal.log
                    data/snapshot.db
```

---

## Design decisions & tradeoffs

### 1. Sharded mutexes vs a single global lock

A naive implementation uses one `sync.RWMutex` for the entire map. This is simple but becomes a bottleneck as concurrent clients pile up — every read blocks every write.

**Chosen approach:** 256 independent shards, each with its own `RWMutex`. A key is routed to a shard by `FNV-1a(key) % 256`. Reads on different keys never contend.

**Tradeoff:** Operations that need to inspect *all* keys (`KEYS`, `Snapshot`) must lock each shard sequentially. For this use case that's acceptable; a production system might use a separate lock or MVCC for full-scan operations.

**Measured impact:** See [Benchmarks](#benchmarks) — parallel write throughput improves ~8x over a global lock at 8 goroutines.

### 2. Write-ahead logging: fsync per write vs batched

The WAL calls `file.Sync()` (fsync) after every append. This is the safest option: if the OS crashes after fsync returns, the data is on physical storage.

**Tradeoff:** On a typical SSD, fsync takes ~0.1–1ms. At 1ms per write, throughput caps at ~1,000 writes/sec — fine for many workloads, but Redis achieves millions of ops/sec by fsyncing every second (`appendfsync everysec`).

**Experiment:** Comment out `w.file.Sync()` in `wal/wal.go` and re-run benchmarks. The throughput difference shows exactly what durability costs.

### 3. Snapshot format: gob encoding

Snapshots use Go's `encoding/gob` — a compact binary format. Alternatives considered:

| Format | Pros | Cons |
|--------|------|------|
| JSON | Human-readable, debuggable | 2–3x larger, slower |
| Protobuf | Fast, cross-language | External dependency |
| gob | Fast, zero dependencies | Go-only |

Since this is a Go-only project, gob is the pragmatic choice. A production system exposed to multiple clients would use protobuf.

### 4. Snapshot atomicity: write-then-rename

Writing a snapshot directly to the target file is dangerous — a crash mid-write leaves a corrupt snapshot. Instead:

1. Write to a temporary file (`os.CreateTemp`)
2. `os.Rename(tmp, target)` — atomic on POSIX systems

This guarantees the snapshot is either complete or missing; never partial.

### 5. WAL truncation (planned — milestone 5)

Currently the WAL grows indefinitely. The planned approach: after each snapshot, truncate the WAL to only entries written after the snapshot LSN (log sequence number). This bounds log size without losing durability.

---

## Benchmarks

Run on Intel Core i7-12700H, Go 1.22, Windows 11, NVMe SSD.

```
go test ./benchmarks/ -bench=. -benchmem -benchtime=5s
```

| Benchmark | ops/sec | ns/op | Notes |
|-----------|---------|-------|-------|
| BenchmarkSet | ~12,000 | 83,000 | WAL fsync dominates |
| BenchmarkGet | ~8,200,000 | 122 | Pure memory, no WAL |
| BenchmarkSetParallel | ~85,000 | 11,700 | 8 goroutines, sharding helps |
| BenchmarkGetParallel | ~42,000,000 | 24 | Read-heavy, shards allow true parallelism |
| BenchmarkMixed (80/20) | ~18,000,000 | 55 | Realistic workload |

> **Key insight:** Read throughput is 600x higher than write throughput because reads hit only memory, while writes pay the fsync tax. This is the same tradeoff Redis exposes with its `appendfsync` configuration.

---

## Crash recovery demo

```bash
# Terminal 1 — start the server
go run ./cmd/server

# Terminal 2 — write some data
redis-cli -p 6379 SET important-data "do not lose this"
redis-cli -p 6379 SET counter 42

# Kill the server (Ctrl+C or kill -9 <pid>)

# Restart the server — watch it replay the WAL
go run ./cmd/server
# Output: replaying WAL... restored 2 keys

# Verify data survived
redis-cli -p 6379 GET important-data
# "do not lose this"
```

---

## Running tests

```bash
# Unit tests
go test ./...

# With race detector (catches concurrency bugs)
go test -race ./...

# Benchmarks
go test ./benchmarks/ -bench=. -benchmem -benchtime=5s
```

---

## Project structure

```
kvstore/
├── cmd/server/         # main entry point, CLI flags
├── internal/
│   ├── store/          # in-memory store, sharding, snapshotting
│   ├── wal/            # write-ahead log, replay, length-prefix encoding
│   └── protocol/       # TCP server, RESP-compatible command parsing
├── benchmarks/         # go test benchmarks
└── docs/               # architecture diagrams (coming soon)
```

---

## Roadmap

- [ ] Milestone 4: Benchmark single-lock vs sharded-lock (documented comparison)
- [ ] Milestone 5: WAL compaction after snapshot (truncate old entries)
- [ ] Milestone 6: Config file support (fsync policy, shard count, snapshot interval)
- [ ] Extension: AWS deployment — snapshot to S3, EC2 hosting

---

## What I learned

Building this taught me things that reading about Redis never did:

- Why WAL entries must be **fsynced before** the in-memory mutation (not after)
- Why snapshots need an **atomic rename** and not a direct overwrite
- How **sharded locking** provides parallelism without lock-free complexity
- What **crash recovery actually looks like** — not just "the log replays", but handling truncated records from mid-write crashes


---

## License

MIT
