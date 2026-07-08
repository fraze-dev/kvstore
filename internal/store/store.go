// Package store implements the in-memory key-value store.
//
// Design decisions:
//   - Sharded mutexes (N shards, each protecting a subset of keys) rather than
//     one global lock. This reduces contention under concurrent workloads.
//     Tradeoff: slightly more complex key routing; SCAN-style ops are costlier.
//   - WAL write happens before the in-memory mutation (write-ahead semantics).
//     If we crash after the WAL write but before the map update, replay fixes it.
//   - Snapshot encodes the full map to disk; on startup we load the snapshot
//     first, then replay only WAL entries that come after the snapshot LSN.
package store

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"os"
	"sync"
	"time"

	"github.com/fraze-dev/kvstore/internal/wal"
)

const numShards = 256 // power of 2 keeps the modulo cheap

// shard holds a subset of keys and its own lock.
type shard struct {
	mu   sync.RWMutex
	data map[string]entry
}

type entry struct {
	Value     string
	ExpiresAt time.Time // zero value means no expiry
}

// Store is the public-facing KV store.
type Store struct {
	shards [numShards]*shard
	w      *wal.WAL
}

// New creates a Store, replays the WAL, and restores from snapshot if present.
func New(w *wal.WAL, snapPath string) (*Store, error) {
	s := &Store{w: w}
	for i := range s.shards {
		s.shards[i] = &shard{data: make(map[string]entry)}
	}

	// Load snapshot first (fast bulk restore)
	if err := s.loadSnapshot(snapPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("snapshot load: %w", err)
	}

	// Replay WAL on top of snapshot (catches mutations since last snapshot)
	if err := w.Replay(s.applyLogEntry); err != nil {
		return nil, fmt.Errorf("WAL replay: %w", err)
	}

	return s, nil
}

// shardFor returns the shard responsible for a given key.
func (s *Store) shardFor(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return s.shards[h.Sum32()%numShards]
}

// Set stores a value. ttl == 0 means no expiry.
func (s *Store) Set(key, value string, ttl time.Duration) error {
	e := entry{Value: value}
	if ttl > 0 {
		e.ExpiresAt = time.Now().Add(ttl)
	}

	// WAL write FIRST — durability guarantee
	if err := s.w.Append(wal.Entry{Op: wal.OpSet, Key: key, Value: value, TTL: ttl}); err != nil {
		return fmt.Errorf("WAL append: %w", err)
	}

	sh := s.shardFor(key)
	sh.mu.Lock()
	sh.data[key] = e
	sh.mu.Unlock()
	return nil
}

// Get retrieves a value. Returns ("", false) if missing or expired.
func (s *Store) Get(key string) (string, bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	e, ok := sh.data[key]
	sh.mu.RUnlock()

	if !ok {
		return "", false
	}
	if !e.ExpiresAt.IsZero() && time.Now().After(e.ExpiresAt) {
		// Lazy expiry — delete on next access
		s.Delete(key) //nolint:errcheck
		return "", false
	}
	return e.Value, true
}

// Delete removes a key. No-op if key doesn't exist.
func (s *Store) Delete(key string) error {
	if err := s.w.Append(wal.Entry{Op: wal.OpDelete, Key: key}); err != nil {
		return fmt.Errorf("WAL append: %w", err)
	}

	sh := s.shardFor(key)
	sh.mu.Lock()
	delete(sh.data, key)
	sh.mu.Unlock()
	return nil
}

// Keys returns all non-expired keys (used for snapshots and debugging).
func (s *Store) Keys() []string {
	var keys []string
	now := time.Now()
	for _, sh := range s.shards {
		sh.mu.RLock()
		for k, e := range sh.data {
			if e.ExpiresAt.IsZero() || now.Before(e.ExpiresAt) {
				keys = append(keys, k)
			}
		}
		sh.mu.RUnlock()
	}
	return keys
}

// Snapshot encodes the full in-memory state to disk using gob encoding.
// This is called periodically and on clean shutdown.
func (s *Store) Snapshot(path string) error {
	f, err := os.CreateTemp("", "kvstore-snap-*")
	if err != nil {
		return err
	}
	tmpName := f.Name()

	// Collect all live entries (holding shard locks briefly per shard)
	snap := make(map[string]entry)
	now := time.Now()
	for _, sh := range s.shards {
		sh.mu.RLock()
		for k, e := range sh.data {
			if e.ExpiresAt.IsZero() || now.Before(e.ExpiresAt) {
				snap[k] = e
			}
		}
		sh.mu.RUnlock()
	}

	if err := gob.NewEncoder(f).Encode(snap); err != nil {
		f.Close()
		os.Remove(tmpName)
		return err
	}
	f.Close()

	// Atomic rename — prevents a half-written snapshot from being read
	return os.Rename(tmpName, path)
}

// loadSnapshot restores store state from a snapshot file.
func (s *Store) loadSnapshot(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var snap map[string]entry
	if err := gob.NewDecoder(f).Decode(&snap); err != nil {
		return err
	}

	now := time.Now()
	for k, e := range snap {
		if e.ExpiresAt.IsZero() || now.Before(e.ExpiresAt) {
			sh := s.shardFor(k)
			sh.data[k] = e
		}
	}
	return nil
}

// applyLogEntry is the callback used during WAL replay.
func (s *Store) applyLogEntry(e wal.Entry) error {
	switch e.Op {
	case wal.OpSet:
		en := entry{Value: e.Value}
		if e.TTL > 0 {
			en.ExpiresAt = time.Now().Add(e.TTL)
		}
		sh := s.shardFor(e.Key)
		sh.mu.Lock()
		sh.data[e.Key] = en
		sh.mu.Unlock()
	case wal.OpDelete:
		sh := s.shardFor(e.Key)
		sh.mu.Lock()
		delete(sh.data, e.Key)
		sh.mu.Unlock()
	}
	return nil
}
