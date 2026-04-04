.PHONY: build test test-verbose test-integration lint clean

build:
	go build ./cmd/...

test:
	go test -race -count=1 ./internal/...

test-verbose:
	go test -race -count=1 -v ./internal/...

test-integration:
	go test -race -count=1 -tags integration -timeout 120s ./tests/integration/...

lint:
	go vet ./...

clean:
	go clean -testcache
	rm -f bin/*
