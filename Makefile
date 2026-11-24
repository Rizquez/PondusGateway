test:
	go test ./... -count=20
	go test -race ./... -count=5
	go vet ./...

build:
	go build ./...

run-flags:
	go run ./cmd/pondus -concurrency 4 -capacity 10 -operations 20 -duration 100ms

run-basic:
	go run ./cmd/pondus -client clients/basic.yaml

run-multiple:
	go run ./cmd/pondus -client clients/multiple.yaml

run-compare:
	go run ./cmd/pondus -client clients/multiple.yaml -compare

run-server:
	go run ./cmd/pondus-server -addr=127.0.0.1:9000 -concurrency=4 -capacity=10 -queue=20 -scheduler=capacity-fit -duration=100ms

run-client:
	go run ./cmd/pondus-client -addr=127.0.0.1:9000 -file=clients/request.json

run-stream-server:
	go run ./cmd/pondus-stream-server -addr=127.0.0.1:9001 -concurrency=4 -capacity=10 -queue=20 -scheduler=capacity-fit -duration=100ms -read-timeout=0

run-stream-client:
	go run ./cmd/pondus-stream-client -addr=127.0.0.1:9001 -file=clients/stream-requests.jsonl -window=4 -timeout=30s
