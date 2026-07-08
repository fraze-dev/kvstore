// Package benchmarks measures KV store performance under various conditions.
//
// Run with:
//
//	go test ./benchmarks/ -bench=. -benchmem -benchtime=5s
//
// Key benchmarks:
//   - BenchmarkSet: pure write throughput (WAL + memory)
//   - BenchmarkGet: pure read throughput (no WAL involved)
//   - BenchmarkSetParallel: concurrent write throughput (shows sharding benefit)
//   - BenchmarkGetParallel: concurrent read throughput
//   - BenchmarkMixed: 80% reads / 20% writes (realistic workload)
package benchmarks

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/fraze-dev/kvstore/internal/store"
	"github.com/fraze-dev/kvstore/internal/wal"
)

// setupStore creates a temporary store for benchmarking.
func setupStore(b *testing.B) (*store.Store, func()) {
	b.Helper()

	walFile, _ := os.CreateTemp("", "bench-wal-*.log")
	snapFile := walFile.Name() + ".snap"
	walFile.Close()

	w, err := wal.New(walFile.Name())
	if err != nil {
		b.Fatal(err)
	}

	s, err := store.New(w, snapFile)
	if err != nil {
		b.Fatal(err)
	}

	cleanup := func() {
		w.Close()
		os.Remove(walFile.Name())
		os.Remove(snapFile)
	}
	return s, cleanup
}

func BenchmarkSet(b *testing.B) {
	s, cleanup := setupStore(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key:%d", i)
		s.Set(key, "value", 0) //nolint:errcheck
	}
}

func BenchmarkGet(b *testing.B) {
	s, cleanup := setupStore(b)
	defer cleanup()

	// Pre-populate
	for i := 0; i < 10_000; i++ {
		s.Set(fmt.Sprintf("key:%d", i), "value", 0) //nolint:errcheck
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get(fmt.Sprintf("key:%d", i%10_000))
	}
}

func BenchmarkSetParallel(b *testing.B) {
	s, cleanup := setupStore(b)
	defer cleanup()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Set(fmt.Sprintf("key:%d", i), "value", 0) //nolint:errcheck
			i++
		}
	})
}

func BenchmarkGetParallel(b *testing.B) {
	s, cleanup := setupStore(b)
	defer cleanup()

	for i := 0; i < 10_000; i++ {
		s.Set(fmt.Sprintf("key:%d", i), "value", 0) //nolint:errcheck
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(fmt.Sprintf("key:%d", i%10_000))
			i++
		}
	})
}

// BenchmarkMixed simulates a realistic 80/20 read-write workload.
func BenchmarkMixed(b *testing.B) {
	s, cleanup := setupStore(b)
	defer cleanup()

	for i := 0; i < 10_000; i++ {
		s.Set(fmt.Sprintf("key:%d", i), "value", 0) //nolint:errcheck
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%5 == 0 { // 20% writes
				s.Set(fmt.Sprintf("key:%d", i%10_000), "newvalue", 0) //nolint:errcheck
			} else { // 80% reads
				s.Get(fmt.Sprintf("key:%d", i%10_000))
			}
			i++
		}
	})
}

// BenchmarkSetWithTTL measures overhead of TTL-aware sets.
func BenchmarkSetWithTTL(b *testing.B) {
	s, cleanup := setupStore(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set(fmt.Sprintf("key:%d", i), "value", 10*time.Second) //nolint:errcheck
	}
}
