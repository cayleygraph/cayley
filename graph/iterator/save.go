package iterator

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph"
)

var (
	_ graph.IteratorFuture = (*Save)(nil)
	_ graph.Tagger         = (*Save)(nil)
)

func Tag(it graph.Iterator, tag string) graph.Iterator {
	if s, ok := it.(graph.Tagger); ok {
		s.AddTags(tag)
		return s
	} else if s, ok := graph.AsShape(it).(graph.TaggerShape); ok {
		s.AddTags(tag)
		return graph.AsLegacy(s)
	}
	return NewSave(it, tag)
}

func TagShape(it graph.IteratorShape, tag string) graph.IteratorShape {
	if s, ok := it.(graph.TaggerShape); ok {
		s.AddTags(tag)
		return s
	} else if s, ok := graph.AsLegacy(it).(graph.Tagger); ok {
		s.AddTags(tag)
		return graph.AsShape(s)
	}
	return newSave(it, tag)
}

func NewSave(on graph.Iterator, tags ...string) *Save {
	it := &Save{
		it: newSave(graph.AsShape(on), tags...),
	}
	it.Iterator = graph.NewLegacy(it.it, it)
	return it
}

type Save struct {
	it *save
	graph.Iterator
}

func (it *Save) AsShape() graph.IteratorShape {
	it.Close()
	return it.it
}

// Add a tag to the iterator.
func (it *Save) AddTags(tag ...string) {
	it.it.AddTags(tag...)
}

func (it *Save) AddFixedTag(tag string, value graph.Ref) {
	it.it.AddFixedTag(tag, value)
}

// Tags returns the tags held in the tagger. The returned value must not be mutated.
func (it *Save) Tags() []string {
	return it.it.Tags()
}

// Fixed returns the fixed tags held in the tagger. The returned value must not be mutated.
func (it *Save) FixedTags() map[string]graph.Ref {
	return it.it.FixedTags()
}

func (it *Save) CopyFromTagger(st graph.TaggerBase) {
	it.it.CopyFromTagger(st)
}

var (
	_ graph.IteratorShapeCompat = (*save)(nil)
	_ graph.TaggerShape         = (*save)(nil)
)

func newSave(on graph.IteratorShape, tags ...string) *save {
	s := &save{it: on}
	s.AddTags(tags...)
	return s
}

type save struct {
	it        graph.IteratorShape
	tags      []string
	fixedTags map[string]graph.Ref
}

func (it *save) Iterate() graph.Scanner {
	return newSaveNext(it.it.Iterate(), it.tags, it.fixedTags)
}

func (it *save) Lookup() graph.Index {
	return newSaveContains(it.it.Lookup(), it.tags, it.fixedTags)
}

func (it *save) AsLegacy() graph.Iterator {
	it2 := &Save{it: it}
	it2.Iterator = graph.NewLegacy(it, it2)
	return it2
}

func (it *save) String() string {
	return fmt.Sprintf("Save(%v, %v)", it.tags, it.fixedTags)
}

// Add a tag to the iterator.
func (it *save) AddTags(tag ...string) {
	it.tags = append(it.tags, tag...)
}

func (it *save) AddFixedTag(tag string, value graph.Ref) {
	if it.fixedTags == nil {
		it.fixedTags = make(map[string]graph.Ref)
	}
	it.fixedTags[tag] = value
}

// Tags returns the tags held in the tagger. The returned value must not be mutated.
func (it *save) Tags() []string {
	return it.tags
}

// Fixed returns the fixed tags held in the tagger. The returned value must not be mutated.
func (it *save) FixedTags() map[string]graph.Ref {
	return it.fixedTags
}

func (it *save) CopyFromTagger(st graph.TaggerBase) {
	it.tags = append(it.tags, st.Tags()...)

	fixed := st.FixedTags()
	if len(fixed) == 0 {
		return
	}
	if it.fixedTags == nil {
		it.fixedTags = make(map[string]graph.Ref, len(fixed))
	}
	for k, v := range fixed {
		it.fixedTags[k] = v
	}
}

func (it *save) Stats(ctx context.Context) (graph.IteratorCosts, error) {
	return it.it.Stats(ctx)
}

func (it *save) Optimize(ctx context.Context) (nit graph.IteratorShape, no bool) {
	sub, ok := it.it.Optimize(ctx)
	if len(it.tags) == 0 && len(it.fixedTags) == 0 {
		return sub, true
	}
	if st, ok2 := sub.(graph.TaggerShape); ok2 {
		st.CopyFromTagger(it)
		return st, true
	} else if st, ok2 := graph.AsLegacy(sub).(graph.Tagger); ok2 {
		st.CopyFromTagger(it)
		return graph.AsShape(st), true
	}
	if !ok {
		return it, false
	}
	s := newSave(sub)
	s.CopyFromTagger(it)
	return s, true
}

func (it *save) SubIterators() []graph.IteratorShape {
	return []graph.IteratorShape{it.it}
}

func newSaveNext(it graph.Scanner, tags []string, fixed map[string]graph.Ref) *saveNext {
	return &saveNext{it: it, tags: tags, fixedTags: fixed}
}

type saveNext struct {
	it        graph.Scanner
	tags      []string
	fixedTags map[string]graph.Ref
}

func (it *saveNext) String() string {
	return fmt.Sprintf("Save(%v, %v)", it.tags, it.fixedTags)
}

func (it *saveNext) TagResults(dst map[string]graph.Ref) {
	it.it.TagResults(dst)

	v := it.Result()
	for _, tag := range it.tags {
		dst[tag] = v
	}

	for tag, value := range it.fixedTags {
		dst[tag] = value
	}
}

func (it *saveNext) Result() graph.Ref {
	return it.it.Result()
}

func (it *saveNext) Next(ctx context.Context) bool {
	return it.it.Next(ctx)
}

func (it *saveNext) NextPath(ctx context.Context) bool {
	return it.it.NextPath(ctx)
}

func (it *saveNext) Err() error {
	return it.it.Err()
}

func (it *saveNext) Close() error {
	return it.it.Close()
}

func newSaveContains(it graph.Index, tags []string, fixed map[string]graph.Ref) *saveContains {
	return &saveContains{it: it, tags: tags, fixed: fixed}
}

type saveContains struct {
	it    graph.Index
	tags  []string
	fixed map[string]graph.Ref
}

func (it *saveContains) String() string {
	return fmt.Sprintf("SaveContains(%v, %v)", it.tags, it.fixed)
}

func (it *saveContains) TagResults(dst map[string]graph.Ref) {
	it.it.TagResults(dst)

	v := it.Result()
	for _, tag := range it.tags {
		dst[tag] = v
	}

	for tag, value := range it.fixed {
		dst[tag] = value
	}
}

func (it *saveContains) Result() graph.Ref {
	return it.it.Result()
}

func (it *saveContains) NextPath(ctx context.Context) bool {
	return it.it.NextPath(ctx)
}

func (it *saveContains) Contains(ctx context.Context, v graph.Ref) bool {
	return it.it.Contains(ctx, v)
}

func (it *saveContains) Err() error {
	return it.it.Err()
}

func (it *saveContains) Close() error {
	return it.it.Close()
}
