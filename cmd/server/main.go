package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/fraze-dev/kvstore/internal/protocol"
	"github.com/fraze-dev/kvstore/internal/store"
	"github.com/fraze-dev/kvstore/internal/wal"
)

func main() {
	// CLI flags — makes the binary configurable without recompiling
	port    := flag.String("port", "6379", "TCP port to listen on")
	walPath := flag.String("wal", "data/wal.log", "Path to write-ahead log file")
	snapPath := flag.String("snap", "data/snapshot.db", "Path to snapshot file")
	flag.Parse()

	// Ensure data directory exists
	if err := os.MkdirAll("data", 0755); err != nil {
		log.Fatalf("failed to create data directory: %v", err)
	}

	// 1. Initialize the WAL (opens or creates the log file)
	w, err := wal.New(*walPath)
	if err != nil {
		log.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	// 2. Initialize the store, replaying WAL + snapshot for crash recovery
	s, err := store.New(w, *snapPath)
	if err != nil {
		log.Fatalf("failed to initialize store: %v", err)
	}

	// 3. Start the TCP server
	srv := protocol.NewServer(*port, s)
	go func() {
		fmt.Printf("kvstore listening on :%s\n", *port)
		fmt.Printf("WAL:      %s\n", *walPath)
		fmt.Printf("Snapshot: %s\n", *snapPath)
		if err := srv.ListenAndServe(); err != nil {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Graceful shutdown on SIGINT / SIGTERM
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("\nShutting down — flushing WAL...")
	if err := s.Snapshot(*snapPath); err != nil {
		log.Printf("snapshot on shutdown failed: %v", err)
	}
	fmt.Println("Done. Bye.")
}
