package iterator

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph"
)

var (
	_ graph.Iterator = (*Save)(nil)
	_ graph.Tagger   = (*Save)(nil)
)

func Tag(it graph.Iterator, tag string) graph.Iterator {
	if s, ok := it.(graph.Tagger); ok {
		s.AddTags(tag)
		return s
	}
	return NewSave(it, tag)
}

func Tag2(it graph.Iterator2, tag string) graph.Iterator2 {
	if s, ok := graph.AsLegacy(it).(graph.Tagger); ok {
		s.AddTags(tag)
		return graph.As2(s)
	}
	return graph.As2(NewSave(graph.AsLegacy(it), tag))
}

func NewSave(on graph.Iterator, tags ...string) *Save {
	s := &Save{it: on}
	s.AddTags(tags...)
	return s
}

type Save struct {
	tags      []string
	fixedTags map[string]graph.Ref
	it        graph.Iterator
}

func (it *Save) String() string {
	return fmt.Sprintf("Save(%v, %v)", it.tags, it.fixedTags)
}

// Add a tag to the iterator.
func (it *Save) AddTags(tag ...string) {
	it.tags = append(it.tags, tag...)
}

func (it *Save) AddFixedTag(tag string, value graph.Ref) {
	if it.fixedTags == nil {
		it.fixedTags = make(map[string]graph.Ref)
	}
	it.fixedTags[tag] = value
}

// Tags returns the tags held in the tagger. The returned value must not be mutated.
func (it *Save) Tags() []string {
	return it.tags
}

// Fixed returns the fixed tags held in the tagger. The returned value must not be mutated.
func (it *Save) FixedTags() map[string]graph.Ref {
	return it.fixedTags
}

func (it *Save) CopyFromTagger(st graph.Tagger) {
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

func (it *Save) TagResults(dst map[string]graph.Ref) {
	it.it.TagResults(dst)

	v := it.Result()
	for _, tag := range it.tags {
		dst[tag] = v
	}

	for tag, value := range it.fixedTags {
		dst[tag] = value
	}
}

func (it *Save) Result() graph.Ref {
	return it.it.Result()
}

func (it *Save) Next(ctx context.Context) bool {
	return it.it.Next(ctx)
}

func (it *Save) NextPath(ctx context.Context) bool {
	return it.it.NextPath(ctx)
}

func (it *Save) Contains(ctx context.Context, v graph.Ref) bool {
	return it.it.Contains(ctx, v)
}

func (it *Save) Err() error {
	return it.it.Err()
}

func (it *Save) Reset() {
	it.it.Reset()
}

func (it *Save) Stats() graph.IteratorStats {
	return it.it.Stats()
}

func (it *Save) Size() (int64, bool) {
	return it.it.Size()
}

func (it *Save) Optimize() (nit graph.Iterator, no bool) {
	sub, ok := it.it.Optimize()
	if len(it.tags) == 0 && len(it.fixedTags) == 0 {
		return sub, true
	}
	if st, ok2 := sub.(graph.Tagger); ok2 {
		st.CopyFromTagger(it)
		return st, true
	}
	if !ok {
		return it, false
	}
	s := NewSave(sub)
	s.CopyFromTagger(it)
	return s, true
}

func (it *Save) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.it}
}

func (it *Save) Close() error {
	return it.it.Close()
}
