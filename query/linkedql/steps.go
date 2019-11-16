package linkedql

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
)

func init() {
	Register(&Vertex{})
	Register(&Placeholder{})
	Register(&View{})
	Register(&Out{})
	Register(&As{})
	Register(&Intersect{})
	Register(&Is{})
	Register(&Back{})
	Register(&ViewBoth{})
	Register(&Count{})
	Register(&Difference{})
	Register(&Filter{})
	Register(&Follow{})
	Register(&FollowReverse{})
	Register(&Has{})
	Register(&HasReverse{})
	Register(&ViewReverse{})
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
	Description() string
}

// IteratorStep is a step that can build an Iterator.
type IteratorStep interface {
	Step
	BuildIterator(qs graph.QuadStore) (query.Iterator, error)
}

// PathStep is a Step that can build a Path.
type PathStep interface {
	Step
	BuildPath(qs graph.QuadStore) (*path.Path, error)
}

// DocumentStep is a Step that can build a DocumentIterator
type DocumentStep interface {
	Step
	BuildDocumentIterator(qs graph.QuadStore) (*DocumentIterator, error)
}

var _ IteratorStep = (*Vertex)(nil)
var _ PathStep = (*Vertex)(nil)

// Vertex corresponds to g.Vertex() and g.V().
type Vertex struct {
	Values []quad.Value `json:"values"`
}

// Type implements Step.
func (s *Vertex) Type() quad.IRI {
	return prefix + "Vertex"
}

// Description implements Step.
func (s *Vertex) Description() string {
	return "Vertex resolves to all the existing objects and primitive values in the graph. If provided with values resolves to a sublist of all the existing values in the graph."
}

// BuildIterator implements IteratorStep
func (s *Vertex) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Vertex) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	return path.StartPath(qs, s.Values...), nil
}

var _ PathStep = (*Placeholder)(nil)

// Placeholder corresponds to .Placeholder().
type Placeholder struct{}

// Type implements Step.
func (s *Placeholder) Type() quad.IRI {
	return "Placeholder"
}

// Description implements Step.
func (s *Placeholder) Description() string {
	return "Placeholder is like Vertex but resolves to the values in the context it is placed in. It should only be used where a PathStep is expected and can't be resolved on its own."
}

// BuildPath implements PathStep.
func (s *Placeholder) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	return path.StartMorphism(), nil
}

var _ IteratorStep = (*View)(nil)
var _ PathStep = (*View)(nil)

// View corresponds to .view().
type View struct {
	From PathStep `json:"from"`
	Via  PathStep `json:"via"`
}

// Type implements Step.
func (s *View) Type() quad.IRI {
	return prefix + "View"
}

// Description implements Step.
func (s *View) Description() string {
	return "View resolves to the values of the given property or properties in via of the current objects. If via is a path it's resolved values will be used as properties."
}

// BuildIterator implements IteratorStep
func (s *View) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *View) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Via.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Out(viaPath), nil
}

var _ IteratorStep = (*Out)(nil)
var _ PathStep = (*Out)(nil)

// Out is an alias for View
type Out struct {
	View
}

// Type implements Step
func (s *Out) Type() quad.IRI {
	return prefix + "Out"
}

// Description implements Step
func (s *Out) Description() string {
	return "Alias for View"
}

var _ IteratorStep = (*As)(nil)
var _ PathStep = (*As)(nil)

// As corresponds to .tag().
type As struct {
	From PathStep `json:"from"`
	Name string   `json:"name"`
}

// Type implements Step.
func (s *As) Type() quad.IRI {
	return prefix + "As"
}

// Description implements Step.
func (s *As) Description() string {
	return "As assigns the resolved values of the from step to a given name. The name can be used with the Select and Documents steps to retreive the values or to return to the values in further steps with the Back step. It resolves to the values of the from step."
}

// BuildIterator implements IteratorStep
func (s *As) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *As) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Tag(s.Name), nil
}

var _ IteratorStep = (*Intersect)(nil)
var _ PathStep = (*Intersect)(nil)

// Intersect represents .intersect() and .and().
type Intersect struct {
	From  PathStep   `json:"from"`
	Steps []PathStep `json:"steps"`
}

// Type implements Step.
func (s *Intersect) Type() quad.IRI {
	return prefix + "Intersect"
}

// Description implements Step.
func (s *Intersect) Description() string {
	return "Intersect resolves to all the same values resolved by the from step and the provided steps."
}

// BuildIterator implements IteratorStep
func (s *Intersect) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
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

var _ IteratorStep = (*Is)(nil)
var _ PathStep = (*Is)(nil)

// Is corresponds to .back().
type Is struct {
	From   PathStep     `json:"from"`
	Values []quad.Value `json:"values"`
}

// Type implements Step.
func (s *Is) Type() quad.IRI {
	return prefix + "Is"
}

// Description implements Step.
func (s *Is) Description() string {
	return "Is resolves to all the values resolved by the from step which are included in provided values."
}

// BuildIterator implements IteratorStep
func (s *Is) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Is) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Is(s.Values...), nil
}

var _ IteratorStep = (*Back)(nil)
var _ PathStep = (*Back)(nil)

// Back corresponds to .back().
type Back struct {
	From PathStep `json:"from"`
	Name string   `json:"name"`
}

// Type implements Step.
func (s *Back) Type() quad.IRI {
	return prefix + "Back"
}

// Description implements Step.
func (s *Back) Description() string {
	return "Back resolves to the values of the previous the step or the values assigned to name in a former step."
}

// BuildIterator implements IteratorStep
func (s *Back) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Back) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Back(s.Name), nil
}

var _ IteratorStep = (*ViewBoth)(nil)
var _ PathStep = (*ViewBoth)(nil)

// ViewBoth corresponds to .viewBoth().
type ViewBoth struct {
	From PathStep `json:"from"`
	Via  PathStep `json:"via"`
}

// Type implements Step.
func (s *ViewBoth) Type() quad.IRI {
	return prefix + "ViewBoth"
}

// Description implements Step.
func (s *ViewBoth) Description() string {
	return "ViewBoth is like View but resolves to both the object values and references to the values of the given properties in via. It is the equivalent for the Union of View and ViewReverse of the same property."
}

// BuildIterator implements IteratorStep
func (s *ViewBoth) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *ViewBoth) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Via.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Both(viaPath), nil
}

var _ IteratorStep = (*Both)(nil)
var _ PathStep = (*Both)(nil)

// Both corresponds to .both()
type Both struct {
	ViewBoth
}

// Type implements Step
func (s *Both) Type() quad.IRI {
	return prefix + "Both"
}

// Description implements Step
func (s *Both) Description() string {
	return prefix + "Alias for ViewBoth"
}

var _ IteratorStep = (*Count)(nil)
var _ PathStep = (*Count)(nil)

// Count corresponds to .count().
type Count struct {
	From PathStep `json:"from"`
}

// Type implements Step.
func (s *Count) Type() quad.IRI {
	return prefix + "Count"
}

// Description implements Step.
func (s *Count) Description() string {
	return "Count resolves to the number of the resolved values of the from step"
}

// BuildIterator implements IteratorStep
func (s *Count) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Count) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Count(), nil
}

var _ IteratorStep = (*Difference)(nil)
var _ PathStep = (*Difference)(nil)

// Difference corresponds to .difference()
type Difference struct {
	From  PathStep   `json:"from"`
	Steps []PathStep `json:"steps"`
}

// Type implements Step.
func (s *Difference) Type() quad.IRI {
	return prefix + "Difference"
}

// Description implements Step.
func (s *Difference) Description() string {
	return "Difference resolves to all the values resolved by the from step different then the values resolved by the provided steps. Caution: it might be slow to execute."
}

// BuildIterator implements IteratorStep
func (s *Difference) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Difference) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	path := fromPath
	for _, step := range s.Steps {
		p, err := step.BuildPath(qs)
		if err != nil {
			return nil, err
		}
		path = path.Except(p)
	}
	return path, nil
}

var _ IteratorStep = (*Filter)(nil)
var _ PathStep = (*Filter)(nil)

// Filter corresponds to filter().
type Filter struct {
	From   PathStep `json:"from"`
	Filter Operator `json:"filter"`
}

// Type implements Step.
func (s *Filter) Type() quad.IRI {
	return prefix + "Filter"
}

// Description implements Step.
func (s *Filter) Description() string {
	return "Apply constraints to a set of nodes. Can be used to filter values by range or match strings."
}

// BuildIterator implements IteratorStep
func (s *Filter) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Filter) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromIt, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return s.Filter.Apply(fromIt)
}

var _ IteratorStep = (*Follow)(nil)
var _ PathStep = (*Follow)(nil)

// Follow corresponds to .follow().
type Follow struct {
	From     PathStep `json:"from"`
	Followed PathStep `json:"followed"`
}

// Type implements Step.
func (s *Follow) Type() quad.IRI {
	return prefix + "Follow"
}

// Description implements Step.
func (s *Follow) Description() string {
	return "The way to use a path prepared with Morphism. Applies the path chain on the morphism object to the current path. Starts as if at the g.M() and follows through the morphism path."
}

// BuildIterator implements IteratorStep
func (s *Follow) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
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

var _ IteratorStep = (*FollowReverse)(nil)
var _ PathStep = (*FollowReverse)(nil)

// FollowReverse corresponds to .followR().
type FollowReverse struct {
	From     PathStep `json:"from"`
	Followed PathStep `json:"followed"`
}

// Type implements Step.
func (s *FollowReverse) Type() quad.IRI {
	return prefix + "FollowReverse"
}

// Description implements Step.
func (s *FollowReverse) Description() string {
	return "The same as follow but follows the chain in the reverse direction. Flips View and ViewReverse where appropriate, the net result being a virtual predicate followed in the reverse direction. Starts at the end of the morphism and follows it backwards (with appropriate flipped directions) to the g.M() location."
}

// BuildIterator implements IteratorStep
func (s *FollowReverse) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
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

var _ IteratorStep = (*Has)(nil)
var _ PathStep = (*Has)(nil)

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

// Description implements Step.
func (s *Has) Description() string {
	return "Filter all paths which are, at this point, on the subject for the given predicate and object, but do not follow the path, merely filter the possible paths. Usually useful for starting with all nodes, or limiting to a subset depending on some predicate/value pair."
}

// BuildIterator implements IteratorStep
func (s *Has) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
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

var _ IteratorStep = (*HasReverse)(nil)
var _ PathStep = (*HasReverse)(nil)

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

// Description implements Step.
func (s *HasReverse) Description() string {
	return "The same as Has, but sets constraint in reverse direction."
}

// BuildIterator implements IteratorStep
func (s *HasReverse) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
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

var _ IteratorStep = (*ViewReverse)(nil)
var _ PathStep = (*ViewReverse)(nil)

// ViewReverse corresponds to .viewReverse().
type ViewReverse struct {
	From PathStep `json:"from"`
	Via  PathStep `json:"via"`
}

// Type implements Step.
func (s *ViewReverse) Type() quad.IRI {
	return prefix + "ViewReverse"
}

// Description implements Step.
func (s *ViewReverse) Description() string {
	return "The inverse of View. Starting with the nodes in `path` on the object, follow the quads with predicates defined by `predicatePath` to their subjects."
}

// BuildIterator implements IteratorStep
func (s *ViewReverse) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *ViewReverse) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Via.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.In(viaPath), nil
}

var _ IteratorStep = (*In)(nil)
var _ PathStep = (*In)(nil)

// In is an alias for ViewReverse
type In struct {
	ViewReverse
}

// Type implements Step
func (s *In) Type() quad.IRI {
	return prefix + "In"
}

// Description implements Step
func (s *In) Description() string {
	return "Alias for ViewReverse"
}

var _ IteratorStep = (*ReversePropertyNames)(nil)
var _ PathStep = (*ReversePropertyNames)(nil)

// ReversePropertyNames corresponds to .reversePropertyNames().
type ReversePropertyNames struct {
	From PathStep `json:"from"`
}

// Type implements Step.
func (s *ReversePropertyNames) Type() quad.IRI {
	return prefix + "ReversePropertyNames"
}

// Description implements Step.
func (s *ReversePropertyNames) Description() string {
	return "Get the list of predicates that are pointing in to a node."
}

// BuildIterator implements IteratorStep
func (s *ReversePropertyNames) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *ReversePropertyNames) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.InPredicates(), nil
}

var _ IteratorStep = (*Labels)(nil)
var _ PathStep = (*Labels)(nil)

// Labels corresponds to .labels().
type Labels struct {
	From PathStep `json:"from"`
}

// Type implements Step.
func (s *Labels) Type() quad.IRI {
	return prefix + "Labels"
}

// Description implements Step.
func (s *Labels) Description() string {
	return "Get the list of inbound and outbound quad labels"
}

// BuildIterator implements IteratorStep
func (s *Labels) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Labels) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Labels(), nil
}

var _ IteratorStep = (*Limit)(nil)
var _ PathStep = (*Limit)(nil)

// Limit corresponds to .limit().
type Limit struct {
	From  PathStep `json:"from"`
	Limit int64    `json:"limit"`
}

// Type implements Step.
func (s *Limit) Type() quad.IRI {
	return prefix + "Limit"
}

// Description implements Step.
func (s *Limit) Description() string {
	return "Limit a number of nodes for current path."
}

// BuildIterator implements IteratorStep
func (s *Limit) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Limit) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Limit(s.Limit), nil
}

var _ IteratorStep = (*PropertyNames)(nil)
var _ PathStep = (*PropertyNames)(nil)

// PropertyNames corresponds to .propertyNames().
type PropertyNames struct {
	From PathStep `json:"from"`
}

// Type implements Step.
func (s *PropertyNames) Type() quad.IRI {
	return prefix + "PropertyNames"
}

// Description implements Step.
func (s *PropertyNames) Description() string {
	return "Get the list of predicates that are pointing out from a node."
}

// BuildIterator implements IteratorStep
func (s *PropertyNames) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *PropertyNames) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.OutPredicates(), nil
}

var _ IteratorStep = (*Properties)(nil)
var _ PathStep = (*Properties)(nil)

// Properties corresponds to .properties().
type Properties struct {
	From  PathStep   `json:"from"`
	Names []quad.IRI `json:"names"`
}

// Type implements Step.
func (s *Properties) Type() quad.IRI {
	return prefix + "Properties"
}

// Description implements Step.
func (s *Properties) Description() string {
	return "Adds tags for all properties of the current entity"
}

// BuildIterator implements IteratorStep
// TODO(iddan): Default tag to Via.
func (s *Properties) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
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

var _ IteratorStep = (*ReversePropertyNamesAs)(nil)
var _ PathStep = (*ReversePropertyNamesAs)(nil)

// ReversePropertyNamesAs corresponds to .reversePropertyNamesAs().
type ReversePropertyNamesAs struct {
	From PathStep `json:"from"`
	Tag  string   `json:"tag"`
}

// Type implements Step.
func (s *ReversePropertyNamesAs) Type() quad.IRI {
	return prefix + "ReversePropertyNamesAs"
}

// Description implements Step.
func (s *ReversePropertyNamesAs) Description() string {
	return "Tag the list of predicates that are pointing in to a node."
}

// BuildIterator implements IteratorStep
func (s *ReversePropertyNamesAs) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *ReversePropertyNamesAs) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.SavePredicates(true, s.Tag), nil
}

var _ IteratorStep = (*PropertyNamesAs)(nil)
var _ PathStep = (*PropertyNamesAs)(nil)

// PropertyNamesAs corresponds to .propertyNamesAs().
type PropertyNamesAs struct {
	From PathStep `json:"from"`
	Tag  string   `json:"tag"`
}

// Type implements Step.
func (s *PropertyNamesAs) Type() quad.IRI {
	return prefix + "PropertyNamesAs"
}

// Description implements Step.
func (s *PropertyNamesAs) Description() string {
	return "Tag the list of predicates that are pointing out from a node."
}

// BuildIterator implements IteratorStep
func (s *PropertyNamesAs) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *PropertyNamesAs) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.SavePredicates(false, s.Tag), nil
}

var _ IteratorStep = (*ReverseProperties)(nil)
var _ PathStep = (*ReverseProperties)(nil)

// ReverseProperties corresponds to .reverseProperties().
type ReverseProperties struct {
	From  PathStep   `json:"from"`
	Names []quad.IRI `json:"names"`
}

// Type implements Step.
func (s *ReverseProperties) Type() quad.IRI {
	return prefix + "ReverseProperties"
}

// Description implements Step.
func (s *ReverseProperties) Description() string {
	return "Gets all the properties the current entity / value is referenced at"
}

// BuildIterator implements IteratorStep
func (s *ReverseProperties) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
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

var _ IteratorStep = (*Skip)(nil)
var _ PathStep = (*Skip)(nil)

// Skip corresponds to .skip().
type Skip struct {
	From   PathStep `json:"from"`
	Offset int64    `json:"offset"`
}

// Type implements Step.
func (s *Skip) Type() quad.IRI {
	return prefix + "Skip"
}

// Description implements Step.
func (s *Skip) Description() string {
	return "Skip a number of nodes for current path."
}

// BuildIterator implements IteratorStep
func (s *Skip) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Skip) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Skip(s.Offset), nil
}

var _ IteratorStep = (*Union)(nil)
var _ PathStep = (*Union)(nil)

// Union corresponds to .union() and .or().
type Union struct {
	From  PathStep   `json:"from"`
	Steps []PathStep `json:"steps"`
}

// Type implements Step.
func (s *Union) Type() quad.IRI {
	return prefix + "Union"
}

// Description implements Step.
func (s *Union) Description() string {
	return "Return the combined paths of the two queries. Notice that it's per-path, not per-node. Once again, if multiple paths reach the same destination, they might have had different ways of getting there (and different tags)."
}

// BuildIterator implements IteratorStep
func (s *Union) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
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

var _ IteratorStep = (*Unique)(nil)
var _ PathStep = (*Unique)(nil)

// Unique corresponds to .unique().
type Unique struct {
	From PathStep `json:"from"`
}

// Type implements Step.
func (s *Unique) Type() quad.IRI {
	return prefix + "Unique"
}

// Description implements Step.
func (s *Unique) Description() string {
	return "Remove duplicate values from the path."
}

// BuildIterator implements IteratorStep
func (s *Unique) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Unique) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Unique(), nil
}

var _ IteratorStep = (*Order)(nil)
var _ PathStep = (*Order)(nil)

// Order corresponds to .order().
type Order struct {
	From PathStep `json:"from"`
}

// Type implements Step.
func (s *Order) Type() quad.IRI {
	return prefix + "Order"
}

// Description implements Step.
func (s *Order) Description() string {
	return "Order sorts the results in ascending order according to the current entity / value"
}

// BuildIterator implements IteratorStep
func (s *Order) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Order) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Order(), nil
}

var _ IteratorStep = (*Optional)(nil)
var _ PathStep = (*Optional)(nil)

// Optional corresponds to .optional().
type Optional struct {
	From PathStep `json:"from"`
	Step PathStep `json:"step"`
}

// Type implements Step.
func (s *Optional) Type() quad.IRI {
	return prefix + "Optional"
}

// Description implements Step.
func (s *Optional) Description() string {
	return "Attempts to follow the given path from the current entity / value, if fails the entity / value will still be kept in the results"
}

// BuildIterator implements IteratorStep
func (s *Optional) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Optional) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	p, err := s.Step.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Optional(p), nil
}

// Where corresponds to .where()
type Where struct {
	From  PathStep   `json:"from"`
	Steps []PathStep `json:"steps"`
}

// Type implements Step.
func (s *Where) Type() quad.IRI {
	return prefix + "Where"
}

// Description implements Step.
func (s *Where) Description() string {
	return "Where applies each provided step in steps in isolation on from"
}

// BuildIterator implements IteratorStep
func (s *Where) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Where) BuildPath(qs graph.QuadStore) (*path.Path, error) {
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
		p = p.And(stepPath.Reverse())
	}
	return p, nil
}
