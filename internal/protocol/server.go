// Package protocol implements a minimal TCP command protocol for the KV store.
//
// Wire format (intentionally simple — easy to test with netcat or telnet):
//
//	SET key value [EX seconds]
//	GET key
//	DEL key
//	PING
//	KEYS
//
// Responses:
//
//	+OK\r\n          — success
//	+PONG\r\n        — response to PING
//	$<len>\r\n       — bulk string follows
//	<value>\r\n
//	$-1\r\n          — nil (key not found)
//	*<n>\r\n         — array of n bulk strings (for KEYS)
//	-ERR <msg>\r\n   — error
//
// This is a deliberate subset of RESP (Redis Serialization Protocol),
// which means redis-cli works as a client out of the box.
package protocol

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/yourusername/kvstore/internal/store"
)

// Server wraps the store and handles TCP connections.
type Server struct {
	port  string
	store *store.Store
}

// NewServer creates a Server.
func NewServer(port string, s *store.Store) *Server {
	return &Server{port: port, store: s}
}

// ListenAndServe accepts connections on the configured port.
func (srv *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", ":"+srv.port)
	if err != nil {
		return err
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go srv.handleConn(conn) // each client gets its own goroutine
	}
}

// handleConn reads commands from a single client connection.
func (srv *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "PING":
			fmt.Fprintf(conn, "+PONG\r\n")

		case "SET":
			// SET key value [EX seconds]
			if len(parts) < 3 {
				fmt.Fprintf(conn, "-ERR wrong number of arguments for SET\r\n")
				continue
			}
			key, value := parts[1], parts[2]
			var ttl time.Duration
			if len(parts) >= 5 && strings.ToUpper(parts[3]) == "EX" {
				secs, err := strconv.Atoi(parts[4])
				if err != nil {
					fmt.Fprintf(conn, "-ERR invalid EX value\r\n")
					continue
				}
				ttl = time.Duration(secs) * time.Second
			}
			if err := srv.store.Set(key, value, ttl); err != nil {
				fmt.Fprintf(conn, "-ERR %s\r\n", err)
				continue
			}
			fmt.Fprintf(conn, "+OK\r\n")

		case "GET":
			if len(parts) < 2 {
				fmt.Fprintf(conn, "-ERR wrong number of arguments for GET\r\n")
				continue
			}
			val, ok := srv.store.Get(parts[1])
			if !ok {
				fmt.Fprintf(conn, "$-1\r\n") // nil bulk string
				continue
			}
			fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(val), val)

		case "DEL":
			if len(parts) < 2 {
				fmt.Fprintf(conn, "-ERR wrong number of arguments for DEL\r\n")
				continue
			}
			if err := srv.store.Delete(parts[1]); err != nil {
				fmt.Fprintf(conn, "-ERR %s\r\n", err)
				continue
			}
			fmt.Fprintf(conn, "+OK\r\n")

		case "KEYS":
			keys := srv.store.Keys()
			fmt.Fprintf(conn, "*%d\r\n", len(keys))
			for _, k := range keys {
				fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(k), k)
			}

		default:
			fmt.Fprintf(conn, "-ERR unknown command '%s'\r\n", cmd)
		}
	}
}
