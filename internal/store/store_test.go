package store_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yourusername/kvstore/internal/store"
	"github.com/yourusername/kvstore/internal/wal"
)

func newTestStore(t *testing.T) (*store.Store, *wal.WAL, string, string) {
	t.Helper()

	walFile, err := os.CreateTemp("", "test-wal-*.log")
	if err != nil {
		t.Fatal(err)
	}
	walPath := walFile.Name()
	walFile.Close()

	snapPath := walPath + ".snap"

	w, err := wal.New(walPath)
	if err != nil {
		t.Fatal(err)
	}

	s, err := store.New(w, snapPath)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		w.Close()
		os.Remove(walPath)
		os.Remove(snapPath)
	})

	return s, w, walPath, snapPath
}

func TestSetAndGet(t *testing.T) {
	s, _, _, _ := newTestStore(t)

	if err := s.Set("hello", "world", 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	val, ok := s.Get("hello")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if val != "world" {
		t.Fatalf("expected 'world', got %q", val)
	}
}

func TestGetMissingKey(t *testing.T) {
	s, _, _, _ := newTestStore(t)
	_, ok := s.Get("nonexistent")
	if ok {
		t.Fatal("expected missing key to return false")
	}
}

func TestDelete(t *testing.T) {
	s, _, _, _ := newTestStore(t)
	s.Set("key", "val", 0) //nolint:errcheck
	s.Delete("key")        //nolint:errcheck

	_, ok := s.Get("key")
	if ok {
		t.Fatal("expected deleted key to be gone")
	}
}

func TestTTLExpiry(t *testing.T) {
	s, _, _, _ := newTestStore(t)
	s.Set("temp", "value", 50*time.Millisecond) //nolint:errcheck

	val, ok := s.Get("temp")
	if !ok || val != "value" {
		t.Fatal("key should exist before TTL expires")
	}

	time.Sleep(100 * time.Millisecond)
	_, ok = s.Get("temp")
	if ok {
		t.Fatal("key should have expired")
	}
}

// TestCrashRecovery is the star of the show:
// write entries, close without snapshotting, reopen — data must survive.
func TestCrashRecovery(t *testing.T) {
	walFile, _ := os.CreateTemp("", "crash-wal-*.log")
	walPath := walFile.Name()
	snapPath := walPath + ".snap"
	walFile.Close()
	defer os.Remove(walPath)
	defer os.Remove(snapPath)

	// First "process" — write some data then "crash" (just close)
	func() {
		w, _ := wal.New(walPath)
		s, _ := store.New(w, snapPath)
		s.Set("survived", "yes", 0)  //nolint:errcheck
		s.Set("also", "survived", 0) //nolint:errcheck
		s.Delete("also")             //nolint:errcheck
		w.Close()
		// No snapshot — simulating a crash
	}()

	// Second "process" — replay WAL from scratch
	w2, err := wal.New(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	s2, err := store.New(w2, snapPath)
	if err != nil {
		t.Fatal(err)
	}

	val, ok := s2.Get("survived")
	if !ok || val != "yes" {
		t.Fatalf("crash recovery failed: expected 'yes', got %q (ok=%v)", val, ok)
	}

	_, ok = s2.Get("also")
	if ok {
		t.Fatal("deleted key should not survive crash recovery")
	}
}

// TestSnapshotAndRestore verifies snapshot + truncated WAL replay works.
func TestSnapshotAndRestore(t *testing.T) {
	s, w, walPath, snapPath := newTestStore(t)

	for i := 0; i < 100; i++ {
		s.Set(fmt.Sprintf("key:%d", i), fmt.Sprintf("val:%d", i), 0) //nolint:errcheck
	}

	if err := s.Snapshot(snapPath); err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}
	w.Close()

	// Reopen — should load from snapshot, not replay 100 WAL entries
	w2, _ := wal.New(walPath)
	defer w2.Close()
	s2, err := store.New(w2, snapPath)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		val, ok := s2.Get(fmt.Sprintf("key:%d", i))
		if !ok || val != fmt.Sprintf("val:%d", i) {
			t.Fatalf("key:%d not restored correctly", i)
		}
	}
}

func TestConcurrentAccess(t *testing.T) {
	s, _, _, _ := newTestStore(t)

	done := make(chan struct{})

	// 10 concurrent writers
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				s.Set(fmt.Sprintf("writer%d:key%d", id, j), "value", 0) //nolint:errcheck
			}
			done <- struct{}{}
		}(i)
	}

	// 10 concurrent readers
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				s.Get(fmt.Sprintf("writer%d:key%d", id, j))
			}
			done <- struct{}{}
		}(i)
	}

	for i := 0; i < 20; i++ {
		<-done
	}
	// If we get here without a data race (run with -race flag), the test passes
}