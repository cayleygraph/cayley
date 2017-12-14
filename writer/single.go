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

package writer

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
)

func init() {
	graph.RegisterWriter("single", NewSingleReplication)
}

type Single struct {
	qs         graph.QuadStore
	ignoreOpts graph.IgnoreOpts
}

func NewSingle(qs graph.QuadStore, opts graph.IgnoreOpts) (graph.QuadWriter, error) {
	return &Single{
		qs:         qs,
		ignoreOpts: opts,
	}, nil
}

func NewSingleReplication(qs graph.QuadStore, opts graph.Options) (graph.QuadWriter, error) {
	var (
		ignoreMissing   bool
		ignoreDuplicate bool
		err             error
	)

	if graph.IgnoreMissing {
		ignoreMissing = true
	} else {
		ignoreMissing, _, err = opts.BoolKey("ignore_missing")
		if err != nil {
			return nil, err
		}
	}

	if graph.IgnoreDuplicates {
		ignoreDuplicate = true
	} else {
		ignoreDuplicate, _, err = opts.BoolKey("ignore_duplicate")
		if err != nil {
			return nil, err
		}
	}

	return NewSingle(qs, graph.IgnoreOpts{
		IgnoreMissing: ignoreMissing,
		IgnoreDup:     ignoreDuplicate,
	})
}

func (s *Single) AddQuad(q quad.Quad) error {
	deltas := make([]graph.Delta, 1)
	deltas[0] = graph.Delta{
		Quad:   q,
		Action: graph.Add,
	}
	return s.qs.ApplyDeltas(deltas, s.ignoreOpts)
}

func (s *Single) AddQuadSet(set []quad.Quad) error {
	deltas := make([]graph.Delta, len(set))
	for i, q := range set {
		deltas[i] = graph.Delta{
			Quad:   q,
			Action: graph.Add,
		}
	}
	return s.qs.ApplyDeltas(deltas, s.ignoreOpts)
}

func (s *Single) RemoveQuad(q quad.Quad) error {
	deltas := make([]graph.Delta, 1)
	deltas[0] = graph.Delta{
		Quad:   q,
		Action: graph.Delete,
	}
	return s.qs.ApplyDeltas(deltas, s.ignoreOpts)
}

// RemoveNode removes all quads with the given value.
//
// It returns ErrNodeNotExists if node is missing.
func (s *Single) RemoveNode(v quad.Value) error {
	gv := s.qs.ValueOf(v)
	if gv == nil {
		return graph.ErrNodeNotExists
	}
	del := graph.NewRemover(s)
	defer del.Close()

	total := 0
	// TODO(dennwc): QuadStore may remove node without iterations. Consider optional interface for this.
	for _, d := range []quad.Direction{quad.Subject, quad.Predicate, quad.Object, quad.Label} {
		r := graph.NewResultReader(s.qs, s.qs.QuadIterator(d, gv))
		n, err := quad.Copy(del, r)
		r.Close()
		if err != nil {
			return err
		}
		total += n
	}
	if err := del.Flush(); err != nil {
		return err
	}
	if total == 0 {
		return graph.ErrNodeNotExists
	}
	return nil
}

func (s *Single) Close() error {
	// Nothing to clean up locally.
	return nil
}

func (s *Single) ApplyTransaction(t *graph.Transaction) error {
	return s.qs.ApplyDeltas(t.Deltas, s.ignoreOpts)
}
