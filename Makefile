.PHONY: build run test test-coverage test-race integration fmt vet clean help

BIN := bin/aero-arc-conformance

build:
	go build -o $(BIN) ./cmd/aero-arc-conformance

run:
	go run ./cmd/aero-arc-conformance --config-path configs/config.yaml

test:
	go test ./...

test-coverage:
	mkdir -p coverage
	go test -coverprofile=coverage/coverage.out ./...

test-race:
	go test -race ./...

integration:
	go test -tags=integration -timeout=10m -v ./internal/integration ./internal/store/postgres

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

vet:
	go vet ./...
	go vet -tags=integration ./...

clean:
	rm -rf bin coverage benchmarks

help:
	@echo "build test test-race integration fmt vet clean"
