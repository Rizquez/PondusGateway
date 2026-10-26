COUNT ?= 100

all-unit-tests:
	go test ./... -count=$(COUNT)
	go test -race ./... -count=$(COUNT)
	go vet ./...

controller-unit-tests:
	go test ./internal/controller -run 'TestSaturation|TestFullQueue|TestFIFO' -count=$(COUNT)

comparison-unit-tests:
	go test ./internal/controller -run 'TestCompareSchedulers' -v