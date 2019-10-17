package iterator

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph/refs"
)

var (
	_ TaggerBase = (*Save)(nil)
)

func Tag(it Shape, tag string) Shape {
	if s, ok := it.(TaggerShape); ok {
		s.AddTags(tag)
		return s
	} else if s, ok := it.(TaggerShape); ok {
		s.AddTags(tag)
		return s
	}
	return NewSave(it, tag)
}

var (
	_ Shape       = (*Save)(nil)
	_ TaggerShape = (*Save)(nil)
)

func NewSave(on Shape, tags ...string) *Save {
	s := &Save{it: on}
	s.AddTags(tags...)
	return s
}

type Save struct {
	it        Shape
	tags      []string
	fixedTags map[string]refs.Ref
}

func (it *Save) Iterate() Scanner {
	return newSaveNext(it.it.Iterate(), it.tags, it.fixedTags)
}

func (it *Save) Lookup() Index {
	return newSaveContains(it.it.Lookup(), it.tags, it.fixedTags)
}

func (it *Save) String() string {
	return fmt.Sprintf("Save(%v, %v)", it.tags, it.fixedTags)
}

// Add a tag to the iterator.
func (it *Save) AddTags(tag ...string) {
	it.tags = append(it.tags, tag...)
}

func (it *Save) AddFixedTag(tag string, value refs.Ref) {
	if it.fixedTags == nil {
		it.fixedTags = make(map[string]refs.Ref)
	}
	it.fixedTags[tag] = value
}

// Tags returns the tags held in the tagger. The returned value must not be mutated.
func (it *Save) Tags() []string {
	return it.tags
}

// Fixed returns the fixed tags held in the tagger. The returned value must not be mutated.
func (it *Save) FixedTags() map[string]refs.Ref {
	return it.fixedTags
}

func (it *Save) CopyFromTagger(st TaggerBase) {
	it.tags = append(it.tags, st.Tags()...)

	fixed := st.FixedTags()
	if len(fixed) == 0 {
		return
	}
	if it.fixedTags == nil {
		it.fixedTags = make(map[string]refs.Ref, len(fixed))
	}
	for k, v := range fixed {
		it.fixedTags[k] = v
	}
}

func (it *Save) Stats(ctx context.Context) (Costs, error) {
	return it.it.Stats(ctx)
}

func (it *Save) Optimize(ctx context.Context) (nit Shape, no bool) {
	sub, ok := it.it.Optimize(ctx)
	if len(it.tags) == 0 && len(it.fixedTags) == 0 {
		return sub, true
	}
	if st, ok2 := sub.(TaggerShape); ok2 {
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

func (it *Save) SubIterators() []Shape {
	return []Shape{it.it}
}

func newSaveNext(it Scanner, tags []string, fixed map[string]refs.Ref) *saveNext {
	return &saveNext{it: it, tags: tags, fixedTags: fixed}
}

type saveNext struct {
	it        Scanner
	tags      []string
	fixedTags map[string]refs.Ref
}

func (it *saveNext) String() string {
	return fmt.Sprintf("Save(%v, %v)", it.tags, it.fixedTags)
}

func (it *saveNext) TagResults(dst map[string]refs.Ref) {
	it.it.TagResults(dst)

	v := it.Result()
	for _, tag := range it.tags {
		dst[tag] = v
	}

	for tag, value := range it.fixedTags {
		dst[tag] = value
	}
}

func (it *saveNext) Result() refs.Ref {
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

func newSaveContains(it Index, tags []string, fixed map[string]refs.Ref) *saveContains {
	return &saveContains{it: it, tags: tags, fixed: fixed}
}

type saveContains struct {
	it    Index
	tags  []string
	fixed map[string]refs.Ref
}

func (it *saveContains) String() string {
	return fmt.Sprintf("SaveContains(%v, %v)", it.tags, it.fixed)
}

func (it *saveContains) TagResults(dst map[string]refs.Ref) {
	it.it.TagResults(dst)

	v := it.Result()
	for _, tag := range it.tags {
		dst[tag] = v
	}

	for tag, value := range it.fixed {
		dst[tag] = value
	}
}

func (it *saveContains) Result() refs.Ref {
	return it.it.Result()
}

func (it *saveContains) NextPath(ctx context.Context) bool {
	return it.it.NextPath(ctx)
}

func (it *saveContains) Contains(ctx context.Context, v refs.Ref) bool {
	return it.it.Contains(ctx, v)
}

func (it *saveContains) Err() error {
	return it.it.Err()
}

func (it *saveContains) Close() error {
	return it.it.Close()
}
