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

package graph

// Define the generic collator used before hashing the value for the quadstore

import (
	"sync"
	"errors"

	"github.com/google/cayley/config"

	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

type Collator struct{
	c *collate.Collator
	buf * collate.Buffer
}

var CollatorPool sync.Pool

func newCollator() *Collator{
	return &Collator{c: collatorPrototype, buf: &collate.Buffer{}}
}

func (self * Collator) Reset() {
	self.buf.Reset()
}

func (self * Collator) KeyCollateStr(str string) []byte {
	if self.c == nil {
		return []byte(str)
	}else{
		return self.c.KeyFromString(self.buf,str)
	}
}

func (self * Collator) KeyCollateBytes(str []byte) []byte {
	if self.c == nil {
		return str
	}else{
		return self.c.Key(self.buf,str)
	}
}

var collatorPrototype * collate.Collator

func InitCollator(cfg * config.Config) error{
	// Init here. Useful only for test
	CollatorPool = sync.Pool{
		New: func() interface{}{ return newCollator() },
	}

	if cfg.CollationType == ""{
		collatorPrototype = nil
		return nil
	}
	lang := language.Make(cfg.CollationType)

	collation_options := make([]collate.Option,0,len(cfg.CollationOptions))

	for _,copts := range(cfg.CollationOptions){
		switch copts {
			case "IgnoreCase":
				collation_options = append(collation_options,collate.IgnoreCase)
			case "IgnoreDiacritics":
				collation_options = append(collation_options,collate.IgnoreDiacritics)
			case "IgnoreWidth":
				collation_options = append(collation_options,collate.IgnoreWidth)
			case "Loose":
				collation_options = append(collation_options,collate.Loose)
			case "Force":
				collation_options = append(collation_options,collate.Force)
			case "Numeric":
				collation_options = append(collation_options,collate.Numeric)
			default:
				return errors.New("Collator: Unknown Option")
		}
	}

	collatorPrototype = collate.New(lang, collation_options...)

	return nil
}