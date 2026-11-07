COUNT ?= 100

run-unit-tests:
	go test ./... -count=$(COUNT)
	go test -race ./... -count=$(COUNT)
	go vet ./...

run-with-flags:
	go run ./cmd/pondus -concurrency 4 -capacity 10 -operations 20 -duration 100ms

run-basic-client:
	go run ./cmd/pondus -client clients/basic.yaml

run-multi-client:
	go run ./cmd/pondus -client clients/multiple.yaml

run-compare:
	go run ./cmd/pondus -client clients/multiple.yaml -compare
