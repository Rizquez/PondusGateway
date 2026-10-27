COUNT ?= 100

unit-tests:
	go test ./... -count=$(COUNT)
	go test -race ./... -count=$(COUNT)
	go vet ./...

default:
	go run ./cmd/pondus -concurrency 4 -capacity 10 -operations 20 -duration 100ms

basic-client:
	go run ./cmd/pondus -config configs/basic.yaml

multi-client:
	go run ./cmd/pondus -config configs/multi-client.yaml

compare:
	go run ./cmd/pondus -config configs/multi-client.yaml -compare
