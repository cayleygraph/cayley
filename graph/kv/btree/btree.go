// Copyright 2017 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package btree

import (
	"github.com/cayleygraph/cayley/graph"
	hkv "github.com/hidal-go/hidalgo/kv"
	"github.com/hidal-go/hidalgo/kv/flat"
	"github.com/hidal-go/hidalgo/kv/flat/btree"
)

const (
	Type = btree.Name
)

func Create(path string, _ graph.Options) (hkv.KV, error) {
	return New(), nil
}

func New() hkv.KV {
	return flat.Upgrade(btree.New())
}
