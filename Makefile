SHELL := /bin/bash

all: \
	commitlint \
	prettier-markdown \
	go-lint \
	go-review \
	go-generate \
	go-test \
	go-mod-tidy \
	git-verify-nodiff

include tools/commitlint/rules.mk
include tools/git-verify-nodiff/rules.mk
include tools/golangci-lint/rules.mk
include tools/goreview/rules.mk
include tools/prettier/rules.mk
include tools/semantic-release/rules.mk

.PHONY: clean
clean:
	$(info [$@] removing generated files...)
	@rm -rf build
	@find . -type f -name '*_gen.go' -exec rm {} \+

.PHONY: go-mod-tidy
go-mod-tidy:
	$(info [$@] tidying Go module files...)
	@go mod tidy -v

.PHONY: go-test
go-test:
	$(info [$@] running Go tests...)
	@mkdir -p build/coverage
	@go test -short -race -coverprofile=build/coverage/$@.txt -covermode=atomic ./...

.PHONY: go-generate
go-generate:
	$(info [$@] generating example code...)
	@go run . generate
