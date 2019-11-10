package linkedql

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
)

func init() {
	Register(&Vertex{})
	Register(&Out{})
	Register(&As{})
	Register(&Intersect{})
	Register(&Is{})
	Register(&Back{})
	Register(&Both{})
	Register(&Count{})
	Register(&Except{})
	Register(&Filter{})
	Register(&Follow{})
	Register(&FollowReverse{})
	Register(&Has{})
	Register(&HasReverse{})
	Register(&In{})
	Register(&ReversePropertyNames{})
	Register(&Labels{})
	Register(&Limit{})
	Register(&PropertyNames{})
	Register(&Properties{})
	Register(&ReversePropertyNamesAs{})
	Register(&PropertyNamesAs{})
	Register(&ReverseProperties{})
	Register(&Skip{})
	Register(&Union{})
	Register(&Unique{})
	Register(&Order{})
}

// Step is the tree representation of a call in a Path context.
// For example:
// g.V(g.IRI("alice")) is represented as &V{ values: quad.Value[]{quad.IRI("alice")} }
// g.V().out(g.IRI("likes")) is represented as &Out{ Via: quad.Value[]{quad.IRI("likes")}, From: &V{} }
type Step interface {
	RegistryItem
	BuildIterator(qs graph.QuadStore) (query.Iterator, error)
}

// PathStep is a Step that cna build a Path.
type PathStep interface {
	BuildPath(qs graph.QuadStore) (*path.Path, error)
}

// DocumentStep is a Step that can build a DocumentIterator
type DocumentStep interface {
	BuildDocumentIterator(qs graph.QuadStore) (*DocumentIterator, error)
}

// Vertex corresponds to g.Vertex() and g.V().
type Vertex struct {
	Values []quad.Value `json:"values"`
}

// Type implements Step.
func (s *Vertex) Type() quad.IRI {
	return prefix + "Vertex"
}

// BuildIterator implements Step.
func (s *Vertex) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Vertex) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	return path.StartPath(qs, s.Values...), nil
}

// Out corresponds to .out().
type Out struct {
	From PathStep `json:"from"`
	Via  PathStep `json:"via"`
	Tags []string `json:"tags"`
}

// Type implements Step.
func (s *Out) Type() quad.IRI {
	return prefix + "Out"
}

// BuildIterator implements Step.
func (s *Out) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Out) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Via.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.OutWithTags(s.Tags, viaPath), nil
}

// As corresponds to .tag().
type As struct {
	From PathStep `json:"from"`
	Tags []string `json:"tags"`
}

// Type implements Step.
func (s *As) Type() quad.IRI {
	return prefix + "As"
}

// BuildIterator implements Step.
func (s *As) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *As) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Tag(s.Tags...), nil
}

// Intersect represents .intersect() and .and().
type Intersect struct {
	From  PathStep   `json:"from"`
	Steps []PathStep `json:"steps"`
}

// Type implements Step.
func (s *Intersect) Type() quad.IRI {
	return prefix + "Intersect"
}

// BuildIterator implements Step.
func (s *Intersect) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Intersect) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	p := fromPath
	for _, step := range s.Steps {
		stepPath, err := step.BuildPath(qs)
		if err != nil {
			return nil, err
		}
		p = p.And(stepPath)
	}
	return p, nil
}

// Is corresponds to .back().
type Is struct {
	From   PathStep     `json:"from"`
	Values []quad.Value `json:"values"`
}

// Type implements Step.
func (s *Is) Type() quad.IRI {
	return prefix + "Is"
}

// BuildIterator implements Step.
func (s *Is) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Is) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Is(s.Values...), nil
}

// Back corresponds to .back().
type Back struct {
	From PathStep `json:"from"`
	Tag  string   `json:"tag"`
}

// Type implements Step.
func (s *Back) Type() quad.IRI {
	return prefix + "Back"
}

// BuildIterator implements Step.
func (s *Back) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Back) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Back(s.Tag), nil
}

// Both corresponds to .both().
type Both struct {
	From PathStep `json:"from"`
	Via  PathStep `json:"via"`
	Tags []string `json:"tags"`
}

// Type implements Step.
func (s *Both) Type() quad.IRI {
	return prefix + "Both"
}

// BuildIterator implements Step.
func (s *Both) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Both) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Via.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.BothWithTags(s.Tags, viaPath), nil
}

// Count corresponds to .count().
type Count struct {
	From PathStep `json:"from"`
}

// Type implements Step.
func (s *Count) Type() quad.IRI {
	return prefix + "Count"
}

// BuildIterator implements Step.
func (s *Count) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Count) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Count(), nil
}

// Except corresponds to .except() and .difference().
type Except struct {
	From     PathStep `json:"from"`
	Excepted PathStep `json:"excepted"`
}

// Type implements Step.
func (s *Except) Type() quad.IRI {
	return prefix + "Except"
}

// BuildIterator implements Step.
func (s *Except) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Except) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	exceptedPath, err := s.Excepted.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Except(exceptedPath), nil
}

// Filter corresponds to filter().
type Filter struct {
	From   PathStep `json:"from"`
	Filter Operator `json:"filter"`
}

// Type implements Step.
func (s *Filter) Type() quad.IRI {
	return prefix + "Filter"
}

// BuildIterator implements Step.
func (s *Filter) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Filter) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromIt, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return s.Filter.Apply(fromIt)
}

// Follow corresponds to .follow().
type Follow struct {
	From     PathStep `json:"from"`
	Followed PathStep `json:"followed"`
}

// Type implements Step.
func (s *Follow) Type() quad.IRI {
	return prefix + "Follow"
}

// BuildIterator implements Step.
func (s *Follow) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Follow) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	p, err := s.Followed.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Follow(p), nil
}

// FollowReverse corresponds to .followR().
type FollowReverse struct {
	From     PathStep `json:"from"`
	Followed PathStep `json:"followed"`
}

// Type implements Step.
func (s *FollowReverse) Type() quad.IRI {
	return prefix + "FollowReverse"
}

// BuildIterator implements Step.
func (s *FollowReverse) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *FollowReverse) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	p, err := s.Followed.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.FollowReverse(p), nil
}

// Has corresponds to .has().
type Has struct {
	From   PathStep     `json:"from"`
	Via    PathStep     `json:"via"`
	Values []quad.Value `json:"values"`
}

// Type implements Step.
func (s *Has) Type() quad.IRI {
	return prefix + "Has"
}

// BuildIterator implements Step.
func (s *Has) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Has) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Via.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Has(viaPath, s.Values...), nil
}

// HasReverse corresponds to .hasR().
type HasReverse struct {
	From   PathStep     `json:"from"`
	Via    PathStep     `json:"via"`
	Values []quad.Value `json:"values"`
}

// Type implements Step.
func (s *HasReverse) Type() quad.IRI {
	return prefix + "HasReverse"
}

// BuildIterator implements Step.
func (s *HasReverse) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *HasReverse) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Via.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.HasReverse(viaPath, s.Values...), nil
}

// In corresponds to .in().
type In struct {
	From PathStep `json:"from"`
	Via  PathStep `json:"via"`
	Tags []string `json:"tags"`
}

// Type implements Step.
func (s *In) Type() quad.IRI {
	return prefix + "In"
}

// BuildIterator implements Step.
func (s *In) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *In) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Via.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.InWithTags(s.Tags, viaPath), nil
}

// ReversePropertyNames corresponds to .reversePropertyNames().
type ReversePropertyNames struct {
	From PathStep `json:"from"`
}

// Type implements Step.
func (s *ReversePropertyNames) Type() quad.IRI {
	return prefix + "ReversePropertyNames"
}

// BuildIterator implements Step.
func (s *ReversePropertyNames) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *ReversePropertyNames) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.InPredicates(), nil
}

// Labels corresponds to .labels().
type Labels struct {
	From PathStep `json:"from"`
}

// Type implements Step.
func (s *Labels) Type() quad.IRI {
	return prefix + "Labels"
}

// BuildIterator implements Step.
func (s *Labels) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Labels) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Labels(), nil
}

// Limit corresponds to .limit().
type Limit struct {
	From  PathStep `json:"from"`
	Limit int64    `json:"limit"`
}

// Type implements Step.
func (s *Limit) Type() quad.IRI {
	return prefix + "Limit"
}

// BuildIterator implements Step.
func (s *Limit) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Limit) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Limit(s.Limit), nil
}

// PropertyNames corresponds to .propertyNames().
type PropertyNames struct {
	From PathStep `json:"from"`
}

// Type implements Step.
func (s *PropertyNames) Type() quad.IRI {
	return prefix + "PropertyNames"
}

// BuildIterator implements Step.
func (s *PropertyNames) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *PropertyNames) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.OutPredicates(), nil
}

// Properties corresponds to .properties().
type Properties struct {
	From  PathStep   `json:"from"`
	Names []quad.IRI `json:"names"`
}

// Type implements Step.
func (s *Properties) Type() quad.IRI {
	return prefix + "Properties"
}

// BuildIterator implements Step.
// TODO(iddan): Default tag to Via.
func (s *Properties) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildDocumentIterator implements DocumentsStep
func (s *Properties) BuildDocumentIterator(qs graph.QuadStore) (*DocumentIterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	it, err := NewValueIterator(p, qs), nil
	if err != nil {
		return nil, err
	}
	return NewDocumentIterator(qs, it.path), nil
}

// BuildPath implements PathStep.
func (s *Properties) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	p := fromPath
	if s.Names != nil {
		for _, name := range s.Names {
			tag := string(name)
			p = p.Save(name, tag)
		}
	} else {
		panic("Not implemented: should tag all properties")
	}
	return p, nil
}

// ReversePropertyNamesAs corresponds to .reversePropertyNamesAs().
type ReversePropertyNamesAs struct {
	From PathStep `json:"from"`
	Tag  string   `json:"tag"`
}

// Type implements Step.
func (s *ReversePropertyNamesAs) Type() quad.IRI {
	return prefix + "ReversePropertyNamesAs"
}

// BuildIterator implements Step.
func (s *ReversePropertyNamesAs) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *ReversePropertyNamesAs) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.SavePredicates(true, s.Tag), nil
}

// PropertyNamesAs corresponds to .propertyNamesAs().
type PropertyNamesAs struct {
	From PathStep `json:"from"`
	Tag  string   `json:"tag"`
}

// Type implements Step.
func (s *PropertyNamesAs) Type() quad.IRI {
	return prefix + "PropertyNamesAs"
}

// BuildIterator implements Step.
func (s *PropertyNamesAs) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *PropertyNamesAs) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.SavePredicates(false, s.Tag), nil
}

// ReverseProperties corresponds to .reverseProperties().
type ReverseProperties struct {
	From  PathStep   `json:"from"`
	Names []quad.IRI `json:"names"`
}

// Type implements Step.
func (s *ReverseProperties) Type() quad.IRI {
	return prefix + "ReverseProperties"
}

// BuildIterator implements Step.
func (s *ReverseProperties) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *ReverseProperties) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	p := fromPath
	for _, name := range s.Names {
		p = fromPath.SaveReverse(name, string(name))
	}
	return p, nil
}

// Skip corresponds to .skip().
type Skip struct {
	From   PathStep `json:"from"`
	Offset int64    `json:"offset"`
}

// Type implements Step.
func (s *Skip) Type() quad.IRI {
	return prefix + "Skip"
}

// BuildIterator implements Step.
func (s *Skip) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Skip) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Skip(s.Offset), nil
}

// Union corresponds to .union() and .or().
type Union struct {
	From  PathStep   `json:"from"`
	Steps []PathStep `json:"steps"`
}

// Type implements Step.
func (s *Union) Type() quad.IRI {
	return prefix + "Union"
}

// BuildIterator implements Step.
func (s *Union) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Union) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	p := fromPath
	for _, step := range s.Steps {
		valuePath, err := step.BuildPath(qs)
		if err != nil {
			return nil, err
		}
		p = p.Or(valuePath)
	}
	return p, nil
}

// Unique corresponds to .unique().
type Unique struct {
	From PathStep `json:"from"`
}

// Type implements Step.
func (s *Unique) Type() quad.IRI {
	return prefix + "Unique"
}

// BuildIterator implements Step.
func (s *Unique) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Unique) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Unique(), nil
}

// Order corresponds to .order().
type Order struct {
	From PathStep `json:"from"`
}

// Type implements Step.
func (s *Order) Type() quad.IRI {
	return prefix + "Order"
}

// BuildIterator implements Step.
func (s *Order) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Order) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Order(), nil
}

// Morphism corresponds to .Morphism().
type Morphism struct{}

// Type implements Step.
func (s *Morphism) Type() quad.IRI {
	return "Morphism"
}

// BuildPath implements ValueStep.
func (s *Morphism) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	return path.StartMorphism(), nil
}

// Optional corresponds to .optional().
type Optional struct {
	From PathStep `json:"from"`
	Path PathStep `json:"path"`
}

// Type implements Step.
func (s *Optional) Type() quad.IRI {
	return "Optional"
}

// BuildIterator implements Step.
func (s *Optional) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	p, err := s.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// BuildPath implements PathStep.
func (s *Optional) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	p, err := s.Path.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Optional(p), nil
}
