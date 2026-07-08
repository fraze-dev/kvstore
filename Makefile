.PHONY: run test bench race clean

run:
	go run ./cmd/server

test:
	go test ./...

# Run tests with the race detector — catches concurrency bugs
race:
	go test -race ./...

bench:
	go test ./benchmarks/ -bench=. -benchmem -benchtime=5s

clean:
	rm -rf data/
