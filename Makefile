COUNT ?= 100

all-unit-tests:
	go test ./... -count=$(COUNT)
	go test -race ./... -count=$(COUNT)
	go vet ./...
