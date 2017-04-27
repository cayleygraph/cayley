#!/bin/sh

GOGOPROTO_ROOT="${GOPATH}/src"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"
protoc --gogofaster_out=. -I=.:"${GOGOPROTO_PATH}" *.proto
