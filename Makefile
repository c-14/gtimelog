.PHONY: all build test

all: build vet fmt

build: gtimelog.go
	go build

fmt: gtimelog.go
	go fmt

vet: gtimelog.go
	go vet

# test: gtimelog_test.go
# 	go test
