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

package elastic

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	elastic "gopkg.in/olivere/elastic.v5"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/internal/lru"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/pquads"
)

// QuadStoreType describes backend
const QuadStoreType = "elastic"
const DefaultESIndex = "cayley"

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc:      newQuadStore,
		UpgradeFunc:  nil,
		InitFunc:     createNewElasticGraph,
		IsPersistent: true,
	})
}

// NodeHash is the hashed value of the Node
type NodeHash string

// IsNode checks if the hashed value corresponds to a Node
func (NodeHash) IsNode() bool { return false }

// Key returns the hashed Node
func (v NodeHash) Key() interface{} { return v }

// QuadHash is the hashed value of the Quad
type QuadHash string

// IsNode checks if the hashed value corresponds to a Quad
func (QuadHash) IsNode() bool { return false }

// Key returns the hashed Quad
func (v QuadHash) Key() interface{} { return v }

// Get returns the value of a Quad direction
// An offset value is set based on the direction passed in and used to lookup in QuadHash
func (v QuadHash) Get(d quad.Direction) string {
	var offset int
	switch d {
	case quad.Subject:
		offset = 0
	case quad.Predicate:
		offset = (quad.HashSize * 2)
	case quad.Object:
		offset = (quad.HashSize * 2) * 2
	case quad.Label:
		offset = (quad.HashSize * 2) * 3
		if len(v) == offset { // no label
			return ""
		}
	}
	return string(v[offset : quad.HashSize*2+offset])
}

// QuadStore stores details needed for backend (elasticsearch in this case)
type QuadStore struct {
	client      *elastic.Client
	nodeTracker *lru.Cache
	sizes       *lru.Cache
}

// dialElastic connects to elasticsearch
func dialElastic(addr string, options graph.Options) (*elastic.Client, error) {
	client, err := elastic.NewClient(elastic.SetURL(addr))
	if err != nil {
		return client, err
	}

	return client, nil
}

// createNewElasticGraph initializes a new Elasticsearch graph and creates the necessary mappings
func createNewElasticGraph(addr string, options graph.Options) error {
	ctx := context.Background()
	client, err := dialElastic(addr, options)
	if err != nil {
		return err
	}
	var settings string
	if val, ok := options["settings"]; ok {
		settings = val.(string)
	} else {
		settings = `
		{
			"number_of_shards":1,
			"number_of_replicas":0
		}`
	}
	allSettings := []string{}
	allSettings = append(allSettings, `{
		"settings":`)
	allSettings = append(allSettings, settings)
	allSettings = append(allSettings, `,
		"mappings": {
		"quads": {
			"properties": {
				"subject": {
					"type": "keyword"
				},
				"object": {
					"type": "keyword"
				},
				"predicate": {
					"type": "keyword"
				},
				"label": {
					"type": "keyword"
				}
			}
		},
		
		"nodes": {
			"properties": {
				"node": {
					"type": "string"
				},
				"hash": {
					"type": "keyword"
				}
			},
			"dynamic_templates": [
				{"strings": {
					"match_mapping_type": "string",
					"mapping": {
						"type": "keyword"
					}
				}}
			]
		}
    }
	}`)
	mapping := strings.Join(allSettings, "")
	_, err = client.CreateIndex(DefaultESIndex).BodyString(mapping).Do(ctx)
	if err != nil {
		return err
	}
	return err
}

func newQuadStore(addr string, options graph.Options) (graph.QuadStore, error) {
	var qs QuadStore
	client, err := dialElastic(addr, options)
	if err != nil {
		return nil, err
	}
	qs.client = client
	qs.nodeTracker = lru.New(1 << 16)
	qs.sizes = lru.New(1 << 16)
	return &qs, nil
}

type value interface{}

// ElasticNode contains Node properties
type ElasticNode struct {
	ID   string `json:"hash"`
	Name value  `json:"node"`
}

func toQuadValue(v value) quad.Value {
	if v == nil {
		return nil
	}

	switch d := v.(type) {
	case string:
		return quad.Raw(d) // compatibility
	case int64:
		return quad.Int(d)
	case float64:
		return quad.Float(d)
	case bool:
		return quad.Bool(d)
	case time.Time:
		return quad.Time(d)
	case map[string]interface{}:
		if _, ok := d["iri"].(bool); ok {
			return quad.IRI(d["val"].(string))
		} else if _, ok := d["bnode"].(bool); ok {
			return quad.BNode(d["val"].(string))
		} else if val, ok := d["lang"].(string); ok {
			return quad.LangString{
				Value: quad.String(d["val"].(string)),
				Lang:  val,
			}
		} else if val, ok := d["type"].(string); ok {
			return quad.TypedString{
				Value: quad.String(d["val"].(string)),
				Type:  quad.IRI(val),
			}
		}
		return quad.String(d["val"].(string))
	case []byte:
		var p pquads.Value
		if err := p.Unmarshal(d); err != nil {
			clog.Errorf("Error: Couldn't decode value: %v", err)
			return nil
		}
		return p.ToNative()
	default:
		panic(fmt.Errorf("unsupported type: %T", v))
	}
}

type elasticString struct {
	Value   string `json:"val"`
	IsIRI   bool   `json:"iri,omitempty"`
	IsBNode bool   `json:"bnode,omitempty"`
	Type    string `json:"type,omitempty"`
	Lang    string `json:"lang,omitempty"`
}

func toElasticValue(v quad.Value) value {
	if v == nil {
		return nil
	}
	fmt.Println("to elastic value-----")
	switch d := v.(type) {
	case quad.Raw:
		return string(d)
	case quad.String:
		return elasticString{Value: string(d)}
	case quad.IRI:
		return elasticString{Value: string(d), IsIRI: true}
	case quad.BNode:
		return elasticString{Value: string(d), IsBNode: true}
	case quad.TypedString:
		return elasticString{Value: string(d.Value), Type: string(d.Type)}
	case quad.LangString:
		return elasticString{Value: string(d.Value), Lang: string(d.Lang)}
	case quad.Int:
		return int64(d)
	case quad.Float:
		return float64(d)
	case quad.Bool:
		return bool(d)
	case quad.Time:
		// TODO(dennwc): mongo supports only ms precision
		// we can alternatively switch to protobuf serialization instead
		// (maybe add an option for this)
		return time.Time(d)
	default:
		qv := pquads.MakeValue(v)
		data, err := qv.Marshal()
		if err != nil {
			panic(err)
		}
		return data
	}
}

func hashOf(s quad.Value) string {
	if s == nil {
		return ""
	}
	return hex.EncodeToString(quad.HashOf(s))
}

func (qs *QuadStore) getIDForQuad(t quad.Quad) string {
	id := hashOf(t.Subject)
	id += hashOf(t.Predicate)
	id += hashOf(t.Object)
	id += hashOf(t.Label)
	return id
}

func (qs *QuadStore) getSize(resultType string, query elastic.Query) (int64, error) {
	ctx := context.Background()

	searchResults, ok := qs.client.Search(DefaultESIndex).
		Type(resultType).
		Query(query).
		Do(ctx)
	src, err := query.Source()
	if err != nil {
		return -1, err
	}
	if ok != nil {
		clog.Errorf("Trouble getting size for iterator! %v", err)
		return -1, nil
	}
	key, err := json.Marshal(src)
	if err != nil {
		return -1, err
	}
	size := searchResults.TotalHits()
	qs.sizes.Put(string(key), int64(size))
	return size, nil
}

func (qs *QuadStore) checkValid(key string) bool {
	// Check if a quad with that key already exists (meaning it is a duplicate delta). If so, return true.
	res, err := qs.client.Get().Index(DefaultESIndex).Type("quads").Id(key).Do(context.Background())
	if err != nil {
		return false
	}
	return res.Found
}

// elasticNodeTracker - Keeping track of nodes in a quad for graph deletes
type elasticNodeTracker struct {
	NodeType  quad.Direction
	DeltaFlag graph.Procedure
}

// ApplyDeltas - A Delta is any update to the graph (an add or delete)
func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {

	// A map of quad.Value (sub, pred, obj, or label) to an elasticNodeTracker struct
	// Used to decide whether to add a node or delete it.
	nodeTracker := make(map[quad.Value]elasticNodeTracker)

	// Loop through all the deltas (graph adds or deletes)
	for _, d := range deltas {
		// Delta action must be add or delete. Else, throw error.
		if d.Action != graph.Add && d.Action != graph.Delete {
			return &graph.DeltaError{Delta: d, Err: graph.ErrInvalidAction}
		}
		key := qs.getIDForQuad(d.Quad)

		switch d.Action {
		case graph.Add:
			if qs.checkValid(key) { // Check if the quad already exists
				// If an option is provided to ignore duplicates, continue. Else, throw an error
				if ignoreOpts.IgnoreDup {
					continue
				} else {
					return &graph.DeltaError{Delta: d, Err: graph.ErrQuadExists}
				}
			}
		case graph.Delete:
			if !qs.checkValid(key) { // Check if the quad doesn't exist
				// If an option is provided to ignore missing, continue. Else, throw an error
				if ignoreOpts.IgnoreMissing {
					continue
				} else {
					return &graph.DeltaError{Delta: d, Err: graph.ErrQuadNotExist}
				}
			}
		}

		// Keeps track of the graph action (add or delete) for each kind of node (sub, pred, obj, label).
		// If d.Action is add, add the node. Else, delete it.
		nodeTracker[d.Quad.Subject] = elasticNodeTracker{
			quad.Subject, d.Action,
		}
		nodeTracker[d.Quad.Object] = elasticNodeTracker{
			quad.Object, d.Action,
		}
		nodeTracker[d.Quad.Predicate] = elasticNodeTracker{
			quad.Predicate, d.Action,
		}

		if d.Quad.Label != nil {
			nodeTracker[d.Quad.Label] = elasticNodeTracker{
				quad.Label, d.Action,
			}
		}
	}

	if clog.V(2) {
		clog.Infof("Existence verified. Proceeding.")
	}

	// Update the log index with the intended graph actions (add or delete)
	for _, d := range deltas {
		err := qs.updateLog(d)
		if err != nil {
			return &graph.DeltaError{Delta: d, Err: err}
		}
	}

	// Create all nodes before writing any quads - concurrent reads may observe broken quads otherwise
	// Loop through the nodes and either add or delete based on the boolean flag.
	for k, v := range nodeTracker {
		err := qs.updateNodeBy(k, v)
		if err != nil {
			return err
		}
	}

	// Loop through deltas and update the quads (either add or delete)
	for _, d := range deltas {
		err := qs.updateQuad(d.Quad, d.Action)
		if err != nil {
			return &graph.DeltaError{Delta: d, Err: err}
		}
	}

	_, err := qs.client.Flush(DefaultESIndex).Do(context.TODO())
	return err
}

// ElasticQuad - Quad structure
type ElasticQuad struct {
	Subject   string `json:"subject"`
	Predicate string `json:"predicate"`
	Object    string `json:"object"`
	Label     string `json:"label"`
}

// ElasticNodeEntry - Node structure
type ElasticNodeEntry struct {
	Hash string `json:"hash"`
	Node value  `json:"node"`
}

// ElasticLogEntry - Log entry structure
type ElasticLogEntry struct {
	Action    string
	Key       string
	Timestamp int64
}

func (qs *QuadStore) updateQuad(q quad.Quad, proc graph.Procedure) error {

	switch proc {
	case graph.Add:
		upsert := ElasticQuad{
			Subject:   hashOf(q.Subject),
			Predicate: hashOf(q.Predicate),
			Object:    hashOf(q.Object),
			Label:     hashOf(q.Label),
		}

		// Add document to index with specified ID
		_, err := qs.client.Index().Index(DefaultESIndex).Type("quads").Id(qs.getIDForQuad(q)).BodyJson(upsert).Do(context.Background())

		if err != nil {
			return err
		}

	case graph.Delete:
		// Delete document from index with specified ID
		_, err := qs.client.Delete().Index(DefaultESIndex).Type("quads").Id(qs.getIDForQuad(q)).Do(context.Background())

		if err != nil {
			return err
		}
	}

	return nil
}

func (qs *QuadStore) updateNodeBy(nodeVal quad.Value, trackedNode elasticNodeTracker) error {
	nodeVals := qs.ValueOf(nodeVal)
	nodeId := string(nodeVals.(NodeHash)) // Get hashed value of node

	switch trackedNode.DeltaFlag {
	case graph.Delete:

		// Construct an Elastic query to check if the Node marked for deletion is present
		// in another Quad. If so, don't delete the Node. If not, delete the Node.
		// Example: nodeType - subject, nodeId - 9328afb
		termQuery := elastic.NewTermQuery(trackedNode.NodeType.String(), nodeId)
		ctx := context.Background()

		// Elasticsearch query checking the quads Type
		searchResult, err := qs.client.Search().
			Index(DefaultESIndex).
			Type("quads").
			Query(termQuery).
			From(0).Size(1).
			Do(ctx)
		if err != nil {
			return err
		}

		// If the Node is present in more than one Quad, don't delete
		if searchResult.Hits.TotalHits > 1 {
			return nil
		}

		// Delete Node from nodes Type in elasticsearch
		_, err = qs.client.Delete().
			Index(DefaultESIndex).
			Type("nodes").
			Id(nodeId).
			Do(context.Background())
		if err != nil {
			return err
		}

	case graph.Add:

		doc := ElasticNodeEntry{
			Hash: nodeId,
			Node: toElasticValue(nodeVal),
		}

		// Add document to index
		_, err := qs.client.Index().Index(DefaultESIndex).Type("nodes").Id(nodeId).BodyJson(doc).Do(context.Background())
		if err != nil {
			return err
		}

	}

	return nil
}

func (qs *QuadStore) updateLog(d graph.Delta) error {
	var action string
	if d.Action == graph.Add {
		action = "Add"
	} else {
		action = "Delete"
	}

	entry := ElasticLogEntry{
		Action:    action,
		Key:       qs.getIDForQuad(d.Quad),
		Timestamp: time.Now().UnixNano(),
	}

	// Add document to log index
	_, err := qs.client.Index().Index("log").Type("elastic").BodyJson(entry).Do(context.Background())

	if err != nil {
		return err
	}
	return nil
}

// Quad builds a quad from the values passed in
func (qs *QuadStore) Quad(v graph.Value) quad.Quad {
	//the old stuff from mongo
	h := v.(QuadHash)
	return quad.Quad{
		Subject:   qs.NameOf(NodeHash(h.Get(quad.Subject))),
		Predicate: qs.NameOf(NodeHash(h.Get(quad.Predicate))),
		Object:    qs.NameOf(NodeHash(h.Get(quad.Object))),
		Label:     qs.NameOf(NodeHash(h.Get(quad.Label))),
	}
}

// QuadIterator returns an iterator over quads
func (qs *QuadStore) QuadIterator(d quad.Direction, val graph.Value) graph.Iterator {
	return NewIterator(qs, "quads", d, val)
}

// NodesAllIterator returns an iterator over all nodes
func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(qs, "nodes")
}

// QuadsAllIterator returns an iterator over all quads
func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator(qs, "quads")
}

// ValueOf returns a Node from the quad value passed in
func (qs *QuadStore) ValueOf(s quad.Value) graph.Value {
	return NodeHash(hashOf(s))
}

// NameOf returns the name of the Node after hashing the graph value and running a search on the backend
func (qs *QuadStore) NameOf(v graph.Value) quad.Value {
	if v == nil {
		return nil
	} else if v, ok := v.(graph.PreFetchedValue); ok {
		return v.NameOf()
	}
	hash := v.(NodeHash)
	if hash == "" {
		return nil
	}
	if val, ok := qs.nodeTracker.Get(string(hash)); ok {
		return val.(quad.Value)
	}

	ctx := context.Background()
	termQuery := elastic.NewTermQuery("hash", string(hash))
	searchResult, err := qs.client.Search().
		Index(DefaultESIndex).
		Type("nodes").
		Query(termQuery).
		Size(1).
		Do(ctx)
	if err != nil {
		clog.Errorf("Error: %v", err)
		return nil
	}

	if searchResult.Hits.TotalHits == 0 {
		return nil
	}

	hit := searchResult.Hits.Hits[0]
	// convert json to object
	var eNode ElasticNode
	err = json.Unmarshal(*hit.Source, &eNode)
	if err != nil {
		return nil
	}
	return toQuadValue(eNode.Name)
}

// Size returns the number of quads in index cayley type quads
func (qs *QuadStore) Size() int64 {
	ctx := context.Background()
	searchResult, err := qs.client.
		Count(DefaultESIndex).
		Type("quads").
		Do(ctx)
	if err != nil {
		return int64(0)
	}

	return searchResult
}

// Horizon returns the number of writes done to the store
func (qs *QuadStore) Horizon() graph.PrimaryKey {
	ctx := context.Background()
	searchResult, err := qs.client.
		Search().
		Index("log").
		Type("elastic").
		Sort("_timestamp", false).
		From(0).Size(1).
		Do(ctx)

	if err != nil {
		return graph.NewSequentialKey(0)
	}

	var eNode ElasticNode
	if searchResult.Hits.TotalHits == 0 {
		return graph.NewSequentialKey(0)
	}

	hit := searchResult.Hits.Hits[0]
	// convert json to object
	err = json.Unmarshal(*hit.Source, &eNode)
	if err != nil {
		return graph.NewSequentialKey(0)
	}

	return graph.NewUniqueKey(eNode.ID)
}

// FixedIterator returns an iterator over a fixed value
func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
}

// Close closes the connection to Elasticsearch
func (qs *QuadStore) Close() error {
	qs.client.CloseIndex(DefaultESIndex)
	return nil
}

// QuadDirection gets the Node corresponding to the quad's direction
func (qs *QuadStore) QuadDirection(in graph.Value, d quad.Direction) graph.Value {
	return NodeHash(in.(QuadHash).Get(d))
}

// Type returns the type of backend (elastic in this case)
func (qs *QuadStore) Type() string {
	return QuadStoreType
}

// end interface implementations
