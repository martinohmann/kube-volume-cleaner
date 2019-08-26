.DEFAULT_GOAL := help

PKG_NAME := github.com/martinohmann/kube-volume-cleaner

.PHONY: help
help:
	@grep -E '^[a-zA-Z-]+:.*?## .*$$' Makefile | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "[32m%-12s[0m %s\n", $$1, $$2}'

.PHONY: deps
deps: ## install go deps
	go mod vendor

.PHONY: build
build: ## build kube-volume-cleaner
	go build \
		-ldflags "-s -w" \
		-o kube-volume-cleaner \
		main.go	

.PHONY: install
install: build ## install kube-volume-cleaner
	cp kube-volume-cleaner $(GOPATH)/bin/

.PHONY: test
test: ## run tests
	go test -race -tags="$(TAGS)" $$(go list ./... | grep -v /vendor/)

.PHONY: vet
vet: ## run go vet
	go vet $$(go list ./... | grep -v /vendor/)

.PHONY: coverage
coverage: ## generate code coverage
	scripts/coverage

.PHONY: misspell
misspell: ## check spelling in go files
	misspell *.go

.PHONY: lint
lint: ## lint go files
	golint ./...
