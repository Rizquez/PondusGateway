COUNT ?= 100

unit-tests:
	go test ./... -count=$(COUNT)
	go test -race ./... -count=$(COUNT)
	go vet ./...

default:
	go run ./cmd/pondus -concurrency 4 -capacity 10 -operations 20 -duration 100ms

basic-client:
	go run ./cmd/pondus -client clients/basic.yaml

multi-client:
	go run ./cmd/pondus -client clients/multiple.yaml

compare:
	go run ./cmd/pondus -client clients/multiple.yaml -compare
