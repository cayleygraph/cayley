#!/usr/bin/env bash
go build -ldflags="-X github.com/cayleygraph/cayley/version.GitHash=$(git rev-parse HEAD | cut -c1-12)" -v ./cmd/cayley