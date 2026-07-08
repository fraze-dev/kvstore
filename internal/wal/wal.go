// Package wal implements a write-ahead log for crash recovery.
//
// Design decisions:
//   - Binary encoding via gob for compactness over JSON.
//   - Each entry is length-prefixed (4 bytes) so we can detect truncated
//     records at the end of the file (partial write on crash).
//   - fsync is called on every write by default. This is the safest option
//     but the slowest. A real system would batch fsync calls — this is a
//     great tradeoff to benchmark and document.
//   - The WAL is append-only. Compaction happens via snapshots (see store.go).
package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Op identifies the type of mutation in a log entry.
type Op uint8

const (
	OpSet    Op = 1
	OpDelete Op = 2
)

// Entry is one record in the WAL.
type Entry struct {
	Op    Op
	Key   string
	Value string        // empty for OpDelete
	TTL   time.Duration // 0 means no expiry
}

// WAL is a write-ahead log backed by a single append-only file.
type WAL struct {
	mu   sync.Mutex
	file *os.File
}

// New opens (or creates) the WAL file at path.
func New(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f}, nil
}

// Append encodes entry e and appends it to the log, then fsyncs.
func (w *WAL) Append(e Entry) error {
	// Encode the entry to a buffer first
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(e); err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	// Length-prefix the record (4 bytes, big-endian)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(buf.Len()))

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("write length: %w", err)
	}
	if _, err := w.file.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("write body: %w", err)
	}

	// fsync — this is the durability guarantee.
	// Tradeoff: safe but expensive (~1ms per write on spinning disk).
	// To benchmark: comment this out and measure throughput improvement.
	return w.file.Sync()
}

// Replay reads all complete entries from the WAL and calls fn for each.
// Truncated records at the end (from a crash mid-write) are silently skipped.
func (w *WAL) Replay(fn func(Entry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to beginning for replay
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var lenBuf [4]byte
	for {
		// Read the 4-byte length prefix
		_, err := io.ReadFull(w.file, lenBuf[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break // clean end or truncated length prefix — stop here
		}
		if err != nil {
			return fmt.Errorf("read length: %w", err)
		}

		size := binary.BigEndian.Uint32(lenBuf[:])

		// Read exactly `size` bytes for the record body
		body := make([]byte, size)
		_, err = io.ReadFull(w.file, body)
		if err == io.ErrUnexpectedEOF {
			break // truncated record body — crash happened mid-write, skip it
		}
		if err != nil {
			return fmt.Errorf("read body: %w", err)
		}

		var e Entry
		if err := gob.NewDecoder(bytes.NewReader(body)).Decode(&e); err != nil {
			return fmt.Errorf("decode entry: %w", err)
		}

		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

// Close flushes and closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.file.Sync() //nolint:errcheck
	return w.file.Close()
}

// Size returns the current WAL file size in bytes.
// Useful for deciding when to trigger a snapshot + truncate cycle.
func (w *WAL) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	info, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
