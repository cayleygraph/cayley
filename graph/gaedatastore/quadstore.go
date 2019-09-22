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

package gaedatastore

import (
	"encoding/hex"
	"errors"
	"math"
	"net/http"
	"time"

	"github.com/cayleygraph/cayley/clog"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/http"
	"github.com/cayleygraph/quad"
)

var _ httpgraph.QuadStore = (*QuadStore)(nil)

const (
	QuadStoreType = "gaedatastore"
	quadKind      = "quad"
	nodeKind      = "node"
)

var (
	// Order of quad fields
	spo = [4]quad.Direction{quad.Subject, quad.Predicate, quad.Object, quad.Label}
)

type QuadStore struct {
	context context.Context
}

type MetadataEntry struct {
	NodeCount int64
	QuadCount int64
}

type Token struct {
	Kind string
	Hash string
}

func (t Token) IsNode() bool     { return t.Kind == nodeKind }
func (t Token) Key() interface{} { return t }

type QuadEntry struct {
	Hash      string
	Added     []int64 `datastore:",noindex"`
	Deleted   []int64 `datastore:",noindex"`
	Subject   string  `datastore:"subject"`
	Predicate string  `datastore:"predicate"`
	Object    string  `datastore:"object"`
	Label     string  `datastore:"label"`
}

type NodeEntry struct {
	Name string
	Size int64
}

type LogEntry struct {
	Action    string
	Key       string
	Timestamp int64
}

func init() {
	graph.RegisterQuadStore("gaedatastore", graph.QuadStoreRegistration{
		NewFunc:      newQuadStore,
		UpgradeFunc:  nil,
		InitFunc:     initQuadStore,
		IsPersistent: true,
	})
}

func initQuadStore(_ string, _ graph.Options) error {
	// TODO (panamafrancis) check appengine datastore for consistency
	return nil
}

func newQuadStore(_ string, options graph.Options) (graph.QuadStore, error) {
	return &QuadStore{}, nil
}

func (qs *QuadStore) createKeyForQuad(q quad.Quad) *datastore.Key {
	id := hashOf(q.Subject)
	id += hashOf(q.Predicate)
	id += hashOf(q.Object)
	id += hashOf(q.Label)
	return qs.createKeyFromToken(&Token{quadKind, id})
}

func hashOf(s quad.Value) string {
	return hex.EncodeToString(quad.HashOf(s))
}

func (qs *QuadStore) createKeyForNode(n quad.Value) *datastore.Key {
	id := hashOf(n)
	return qs.createKeyFromToken(&Token{nodeKind, id})
}

func (qs *QuadStore) createKeyForMetadata() *datastore.Key {
	return qs.createKeyFromToken(&Token{"metadata", "metadataentry"})
}

func (qs *QuadStore) createKeyForLog() *datastore.Key {
	return datastore.NewKey(qs.context, "logentry", "", 0, nil)
}

func (qs *QuadStore) createKeyFromToken(t *Token) *datastore.Key {
	return datastore.NewKey(qs.context, t.Kind, t.Hash, 0, nil)
}

func (qs *QuadStore) checkValid(k *datastore.Key) (bool, error) {
	var q QuadEntry
	err := datastore.Get(qs.context, k, &q)
	if err == datastore.ErrNoSuchEntity {
		return false, nil
	}
	if _, ok := err.(*datastore.ErrFieldMismatch); ok {
		return true, nil
	}
	if err != nil {
		clog.Warningf("Error occurred when getting quad/node %s %v", k, err)
		return false, err
	}
	// a deleted node should not be returned as found.
	if len(q.Deleted) >= len(q.Added) {
		return false, nil
	}
	return true, nil
}

func getContext(opts graph.Options) (context.Context, error) {
	req := opts["HTTPRequest"].(*http.Request)
	if req == nil {
		err := errors.New("HTTP Request needed")
		clog.Errorf("%v", err)
		return nil, err
	}
	return appengine.NewContext(req), nil
}

func (qs *QuadStore) ForRequest(r *http.Request) (graph.QuadStore, error) {
	return &QuadStore{context: appengine.NewContext(r)}, nil
}

func (qs *QuadStore) NewQuadWriter() (quad.WriteCloser, error) {
	return &quadWriter{qs: qs}, nil
}

type quadWriter struct {
	qs     *QuadStore
	deltas []graph.Delta
}

func (w *quadWriter) WriteQuad(q quad.Quad) error {
	_, err := w.WriteQuads([]quad.Quad{q})
	return err
}

func (w *quadWriter) WriteQuads(buf []quad.Quad) (int, error) {
	// TODO(dennwc): write an optimized implementation
	w.deltas = w.deltas[:0]
	if cap(w.deltas) < len(buf) {
		w.deltas = make([]graph.Delta, 0, len(buf))
	}
	for _, q := range buf {
		w.deltas = append(w.deltas, graph.Delta{
			Quad: q, Action: graph.Add,
		})
	}
	err := w.qs.ApplyDeltas(w.deltas, graph.IgnoreOpts{
		IgnoreDup: true,
	})
	w.deltas = w.deltas[:0]
	if err != nil {
		return 0, err
	}
	return len(buf), nil
}

func (w *quadWriter) Close() error {
	w.deltas = nil
	return nil
}

func (qs *QuadStore) ApplyDeltas(in []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	if qs.context == nil {
		return errors.New("No context, graph not correctly initialised")
	}
	toKeep := make([]graph.Delta, 0)
	for _, d := range in {
		if d.Action != graph.Add && d.Action != graph.Delete {
			//Defensive shortcut
			return errors.New("Datastore: invalid action")
		}
		key := qs.createKeyForQuad(d.Quad)
		keep := false
		switch d.Action {
		case graph.Add:
			found, err := qs.checkValid(key)
			if err != nil {
				return err
			}
			if found {
				if !ignoreOpts.IgnoreDup {
					return graph.ErrQuadExists
				}
			} else {
				keep = true
			}
		case graph.Delete:
			found, err := qs.checkValid(key)
			if err != nil {
				return err
			}
			if found || ignoreOpts.IgnoreMissing {
				keep = true
			} else {
				return graph.ErrQuadNotExist
			}
		default:
			keep = false
		}
		if keep {
			toKeep = append(toKeep, d)
		}
	}
	if len(toKeep) == 0 {
		return nil
	}
	ids, err := qs.updateLog(toKeep)
	if err != nil {
		clog.Errorf("Updating log failed %v", err)
		return err
	}

	if clog.V(2) {
		clog.Infof("Existence verified. Proceeding.")
	}

	quadsAdded, err := qs.updateQuads(toKeep, ids)
	if err != nil {
		clog.Errorf("UpdateQuads failed %v", err)
		return err
	}
	nodesAdded, err := qs.updateNodes(toKeep)
	if err != nil {
		clog.Warningf("UpdateNodes failed %v", err)
		return err
	}
	err = qs.updateMetadata(quadsAdded, nodesAdded)
	if err != nil {
		clog.Warningf("UpdateMetadata failed %v", err)
		return err
	}
	return nil
}

func (qs *QuadStore) updateNodes(in []graph.Delta) (int64, error) {
	// Collate changes to each node
	var countDelta int64
	var nodesAdded int64
	nodeDeltas := make(map[quad.Value]int64)
	for _, d := range in {
		if d.Action == graph.Add {
			countDelta = 1
		} else {
			countDelta = -1
		}
		nodeDeltas[d.Quad.Subject] += countDelta
		nodeDeltas[d.Quad.Object] += countDelta
		nodeDeltas[d.Quad.Predicate] += countDelta
		if d.Quad.Label != nil {
			nodeDeltas[d.Quad.Label] += countDelta
		}
		nodesAdded += countDelta
	}
	// Create keys and new nodes
	keys := make([]*datastore.Key, 0, len(nodeDeltas))
	tempNodes := make([]NodeEntry, 0, len(nodeDeltas))
	for k, v := range nodeDeltas {
		keys = append(keys, qs.createKeyForNode(k))
		tempNodes = append(tempNodes, NodeEntry{k.String(), v})
	}
	// In accordance with the appengine datastore spec, cross group transactions
	// like these can only be done in batches of 5
	for i := 0; i < len(nodeDeltas); i += 5 {
		j := int(math.Min(float64(len(nodeDeltas)-i), 5))
		foundNodes := make([]NodeEntry, j)
		err := datastore.RunInTransaction(qs.context, func(c context.Context) error {
			err := datastore.GetMulti(c, keys[i:i+j], foundNodes)
			// Sift through for errors
			if me, ok := err.(appengine.MultiError); ok {
				for _, merr := range me {
					if merr != nil && merr != datastore.ErrNoSuchEntity {
						clog.Errorf("Error: %v", merr)
						return merr
					}
				}
			}
			// Carry forward the sizes of the nodes from the datastore
			for k, _ := range foundNodes {
				if foundNodes[k].Name != "" {
					tempNodes[i+k].Size += foundNodes[k].Size
				}
			}
			_, err = datastore.PutMulti(c, keys[i:i+j], tempNodes[i:i+j])
			return err
		}, &datastore.TransactionOptions{XG: true})
		if err != nil {
			clog.Errorf("Error: %v", err)
			return 0, err
		}
	}

	return nodesAdded, nil
}

func (qs *QuadStore) updateQuads(in []graph.Delta, ids []int64) (int64, error) {
	keys := make([]*datastore.Key, 0, len(in))
	for _, d := range in {
		keys = append(keys, qs.createKeyForQuad(d.Quad))
	}
	var quadCount int64
	for i := 0; i < len(in); i += 5 {
		// Find the closest batch of 5
		j := int(math.Min(float64(len(in)-i), 5))
		err := datastore.RunInTransaction(qs.context, func(c context.Context) error {
			foundQuads := make([]QuadEntry, j)
			// We don't process errors from GetMulti as they don't mean anything,
			// we've handled existing quad conflicts above and we overwrite everything again anyways
			datastore.GetMulti(c, keys, foundQuads)
			for k, _ := range foundQuads {
				x := i + k
				foundQuads[k].Hash = keys[x].StringID()
				foundQuads[k].Subject = in[x].Quad.Subject.String()
				foundQuads[k].Predicate = in[x].Quad.Predicate.String()
				foundQuads[k].Object = in[x].Quad.Object.String()
				foundQuads[k].Label = quad.StringOf(in[x].Quad.Label)

				// If the quad exists the Added[] will be non-empty
				if in[x].Action == graph.Add {
					foundQuads[k].Added = append(foundQuads[k].Added, ids[x])
					quadCount += 1
				} else {
					foundQuads[k].Deleted = append(foundQuads[k].Deleted, ids[x])
					quadCount -= 1
				}
			}
			_, err := datastore.PutMulti(c, keys[i:i+j], foundQuads)
			return err
		}, &datastore.TransactionOptions{XG: true})
		if err != nil {
			return 0, err
		}
	}
	return quadCount, nil
}

func (qs *QuadStore) updateMetadata(quadsAdded int64, nodesAdded int64) error {
	key := qs.createKeyForMetadata()
	foundMetadata := new(MetadataEntry)
	err := datastore.RunInTransaction(qs.context, func(c context.Context) error {
		err := datastore.Get(c, key, foundMetadata)
		if err != nil && err != datastore.ErrNoSuchEntity {
			clog.Errorf("Error: %v", err)
			return err
		}
		foundMetadata.QuadCount += quadsAdded
		foundMetadata.NodeCount += nodesAdded
		_, err = datastore.Put(c, key, foundMetadata)
		if err != nil {
			clog.Errorf("Error: %v", err)
		}
		return err
	}, nil)
	return err
}

func (qs *QuadStore) updateLog(in []graph.Delta) ([]int64, error) {
	if qs.context == nil {
		err := errors.New("Error updating log, context is nil, graph not correctly initialised")
		return nil, err
	}
	if len(in) == 0 {
		return nil, errors.New("Nothing to log")
	}
	logEntries := make([]LogEntry, 0, len(in))
	logKeys := make([]*datastore.Key, 0, len(in))
	for _, d := range in {
		var action string
		if d.Action == graph.Add {
			action = "Add"
		} else {
			action = "Delete"
		}

		entry := LogEntry{
			Action:    action,
			Key:       qs.createKeyForQuad(d.Quad).String(),
			Timestamp: time.Now().UnixNano(),
		}
		logEntries = append(logEntries, entry)
		logKeys = append(logKeys, qs.createKeyForLog())
	}

	ids, err := datastore.PutMulti(qs.context, logKeys, logEntries)
	if err != nil {
		clog.Errorf("Error updating log: %v", err)
		return nil, err
	}
	out := make([]int64, 0, len(ids))
	for _, id := range ids {
		out = append(out, id.IntID())
	}
	return out, nil
}

func (qs *QuadStore) QuadIterator(dir quad.Direction, v graph.Ref) graph.Iterator {
	return NewIterator(qs, quadKind, dir, v)
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(qs, nodeKind)
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator(qs, quadKind)
}

func (qs *QuadStore) ValueOf(s quad.Value) graph.Ref {
	id := hashOf(s)
	return &Token{Kind: nodeKind, Hash: id}
}

func (qs *QuadStore) NameOf(val graph.Ref) quad.Value {
	if qs.context == nil {
		clog.Errorf("Error in NameOf, context is nil, graph not correctly initialised")
		return nil
	} else if v, ok := val.(graph.PreFetchedValue); ok {
		return v.NameOf()
	}
	var key *datastore.Key
	if t, ok := val.(*Token); ok && t.Kind == nodeKind {
		key = qs.createKeyFromToken(t)
	} else {
		clog.Errorf("Token not valid")
		return nil
	}

	// TODO (panamafrancis) implement a cache

	node := new(NodeEntry)
	err := datastore.Get(qs.context, key, node)
	if err != nil {
		clog.Errorf("Error: %v", err)
		return nil
	}
	return quad.Raw(node.Name)
}

func (qs *QuadStore) Quad(val graph.Ref) quad.Quad {
	if qs.context == nil {
		clog.Errorf("Error fetching quad, context is nil, graph not correctly initialised")
		return quad.Quad{}
	}
	var key *datastore.Key
	if t, ok := val.(*Token); ok && t.Kind == quadKind {
		key = qs.createKeyFromToken(t)
	} else {
		clog.Errorf("Token not valid")
		return quad.Quad{}
	}

	q := new(QuadEntry)
	err := datastore.Get(qs.context, key, q)
	if err != nil {
		// Red herring error : ErrFieldMismatch can happen when a quad exists but a field is empty
		if _, ok := err.(*datastore.ErrFieldMismatch); !ok {
			clog.Errorf("Error: %v", err)
		}
	}
	var label interface{}
	if q.Label != "" {
		label = q.Label
	}
	return quad.Make(
		q.Subject,
		q.Predicate,
		q.Object,
		label,
	)
}

func (qs *QuadStore) Stats(ctx context.Context, exact bool) (graph.Stats, error) {
	if qs.context == nil {
		return graph.Stats{}, errors.New("error fetching size, context is nil, graph not correctly initialised")
	}
	key := qs.createKeyForMetadata()
	m := new(MetadataEntry)
	err := datastore.Get(qs.context, key, m)
	if err != nil {
		return graph.Stats{}, err
	}
	return graph.Stats{
		Nodes: graph.Size{
			Size:  m.NodeCount,
			Exact: true,
		},
		Quads: graph.Size{
			Size:  m.QuadCount,
			Exact: true,
		},
	}, nil
}

func (qs *QuadStore) QuadIteratorSize(ctx context.Context, d quad.Direction, val graph.Ref) (graph.Size, error) {
	t, ok := val.(*Token)
	if !ok || t.Kind != nodeKind {
		return graph.Size{Size: 0, Exact: true}, nil
	} else if qs.context == nil {
		return graph.Size{}, errors.New("cannot count iterator without a valid context")
	}
	key := qs.createKeyFromToken(t)
	n := new(NodeEntry)
	err := datastore.Get(qs.context, key, n)
	if err != nil && err != datastore.ErrNoSuchEntity {
		return graph.Size{}, err
	}
	return graph.Size{Size: n.Size, Exact: true}, nil
}

func (qs *QuadStore) Close() error {
	qs.context = nil
	return nil
}

func (qs *QuadStore) QuadDirection(val graph.Ref, dir quad.Direction) graph.Ref {
	t, ok := val.(*Token)
	if !ok {
		clog.Errorf("Token not valid")
		return nil
	}
	if t.Kind == nodeKind {
		clog.Errorf("Node tokens not valid")
		return nil
	}
	var offset int
	switch dir {
	case quad.Subject:
		offset = 0
	case quad.Predicate:
		offset = (quad.HashSize * 2)
	case quad.Object:
		offset = (quad.HashSize * 2) * 2
	case quad.Label:
		offset = (quad.HashSize * 2) * 3
	}
	sub := t.Hash[offset : offset+(quad.HashSize*2)]
	return &Token{Kind: nodeKind, Hash: sub}
}
