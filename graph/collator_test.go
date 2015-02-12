// Copyright 2014 The Cayley Authors. All rights reserved.
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

package graph_test

import (
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/config"
)

func initCollator(ctype string, copts []string) error{
	cfg := &config.Config{
		CollationType: ctype,
		CollationOptions: copts,
	}
	return graph.InitCollator(cfg)
}

func TestCollatorLoose(t *testing.T) {
	ret := initCollator("en_us", []string{"Loose"})
	if ret != nil {
		t.Error("Not correctly initialized")
		return
	}
	col1 := graph.CollatorPool.Get().(*graph.Collator)
	col1.Reset()
	defer graph.CollatorPool.Put(col1)

	col2 := graph.CollatorPool.Get().(*graph.Collator)
	col2.Reset()
	defer graph.CollatorPool.Put(col2)

	col3 := graph.CollatorPool.Get().(*graph.Collator)
	col3.Reset()
	defer graph.CollatorPool.Put(col3)

	str1 := col1.KeyCollateStr("Test")
	str2 := col2.KeyCollateStr("test")
	str3 := col3.KeyCollateStr("Tést")
	if string(str1) != string(str2) {
		t.Error("Str1 and Str2 doesn't match")
	}
	if string(str2) != string(str3) {
		t.Error("Str2 and Str3 doesn't match")
	}
}

func TestCollatorStrict(t *testing.T) {
	ret := initCollator("en_us", []string{"Force"})
	if ret != nil {
		t.Error("Not correctly initialized")
		return
	}

	col1 := graph.CollatorPool.Get().(*graph.Collator)
	col1.Reset()
	defer graph.CollatorPool.Put(col1)

	col2 := graph.CollatorPool.Get().(*graph.Collator)
	col2.Reset()
	defer graph.CollatorPool.Put(col2)

	col3 := graph.CollatorPool.Get().(*graph.Collator)
	col3.Reset()
	defer graph.CollatorPool.Put(col3)

	str1 := col1.KeyCollateStr("Test")
	str2 := col2.KeyCollateStr("test")
	str3 := col3.KeyCollateStr("Tést")
	if string(str1) == string(str2) {
		t.Error("Str1 and Str2 match")
	}
	if string(str2) == string(str3) {
		t.Error("Str2 and Str3 match")
	}
}