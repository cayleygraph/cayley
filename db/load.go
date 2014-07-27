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

package db

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/barakmich/glog"
	"github.com/google/cayley/config"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/nquads"
	"io"
	"os"
)

var Stdin = flag.Bool("stdin", false, "Whether or not to load data from standard in")

func Load(ts graph.TripleStore, cfg *config.Config, path string) error {
	var f *os.File
	var err error
	var r io.Reader

	if *Stdin {
		f = os.Stdin
		r = bufio.NewReader(f)
		glog.Infof("Opening database from stdin")
	} else {
		f, err = os.Open(path)
		if err != nil {
			return fmt.Errorf("could not open file %q: %v", path, err)
		}
		defer f.Close()
		r, err = decompressor(f)
		if err != nil {
			glog.Fatalln(err)
		}

	}

	dec := nquads.NewDecoder(r)

	bulker, canBulk := ts.(graph.BulkLoader)
	if canBulk {
		err = bulker.BulkLoad(dec)
		if err == nil {
			return nil
		}
		if err == graph.ErrCannotBulkLoad {
			err = nil
		}
	}
	if err != nil {
		return err
	}

	block := make([]*graph.Triple, 0, cfg.LoadSize)
	for {
		t, err := dec.Unmarshal()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		block = append(block, t)
		if len(block) == cap(block) {
			ts.AddTripleSet(block)
			block = block[:0]
		}
	}
	ts.AddTripleSet(block)

	return nil
}

const (
	gzipMagic  = "\x1f\x8b"
	b2zipMagic = "BZh"
)

type readAtReader interface {
	io.Reader
	io.ReaderAt
}

func decompressor(r readAtReader) (io.Reader, error) {
	var buf [3]byte
	_, err := r.ReadAt(buf[:], 0)
	if err != nil {
		return nil, err
	}
	switch {
	case bytes.Compare(buf[:2], []byte(gzipMagic)) == 0:
		return gzip.NewReader(r)
	case bytes.Compare(buf[:3], []byte(b2zipMagic)) == 0:
		return bzip2.NewReader(r), nil
	default:
		return r, nil
	}
}
