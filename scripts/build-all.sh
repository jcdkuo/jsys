#!/usr/bin/env sh
set -eu

mkdir -p dist
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -o dist/jsys-darwin-arm64 .
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -o dist/jsys-linux-amd64 .
