COUNT ?= 100

all-unit-tests:
	go test ./... -count=$(COUNT)
	go test -race ./... -count=$(COUNT)
	go vet ./...

admission-unit-tests:
	go test ./internal/admission -run 'TestSaturation|TestFullQueue|TestFIFO' -count=$(COUNT)