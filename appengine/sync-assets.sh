#!/bin/bash

SRC="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

rsync -avP --delete $GOPATH/src/github.com/cayleygraph/cayley/{static,docs,templates} $SRC/
