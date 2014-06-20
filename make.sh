#!/usr/bin/env bash

# Copyright 2014 The Cayley Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -e

cd "`dirname '$0'`"
SCRIPTPATH="`pwd`"
cd - > /dev/null

export GOPATH=$SCRIPTPATH
export GOBIN=

function deps {
echo "Fetching dependencies to $SCRIPTPATH..."
printf "                  (00/15)\r"
  go get -u -t github.com/smartystreets/goconvey
printf "#                 (01/15)\r"
  go get -u github.com/badgerodon/peg
printf "##                (02/15)\r"
  go get -u github.com/barakmich/glog
printf "####              (03/15)\r"
  go get -u github.com/julienschmidt/httprouter
printf "#####             (04/15)\r"
  go get -u github.com/petar/GoLLRB/llrb
printf "######            (05/15)\r"
  go get -u github.com/robertkrimen/otto
printf "#######           (06/15)\r"
  go get -u github.com/stretchrcom/testify
printf "########          (07/15)\r"
  go get -u github.com/syndtr/goleveldb/leveldb
printf "#########         (08/15)\r"
  go get -u github.com/syndtr/goleveldb/leveldb/cache
printf "##########        (09/15)\r"
  go get -u github.com/syndtr/goleveldb/leveldb/iterator
printf "###########       (10/15)\r"
  go get -u github.com/syndtr/goleveldb/leveldb/opt
printf "############      (11/15)\r"
  go get -u github.com/syndtr/goleveldb/leveldb/util
printf "#############     (12/15)\r"
  go get -u labix.org/v2/mgo
printf "##############    (13/15)\r"
  go get -u labix.org/v2/mgo/bson
printf "###############   (14/15)\r"
  go get -u github.com/russross/blackfriday
printf "################  (15/15)\r"
printf "\n"
}

function build {
  go build cayley
}

$1
