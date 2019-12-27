package linkedql

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
)

func init() {
	Register(&Entity{})
	Register(&Entities{})
	Register(&Vertex{})
	Register(&Placeholder{})
	Register(&Visit{})
	Register(&Out{})
	Register(&As{})
	Register(&Intersect{})
	Register(&Is{})
	Register(&Back{})
	Register(&Both{})
	Register(&Count{})
	Register(&Difference{})
	Register(&Filter{})
	Register(&Follow{})
	Register(&FollowReverse{})
	Register(&Has{})
	Register(&HasReverse{})
	Register(&VisitReverse{})
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
	Register(&Optional{})
	Register(&Where{})
	Register(&LessThan{})
	Register(&LessThanEquals{})
	Register(&GreaterThan{})
	Register(&GreaterThanEquals{})
}

// Step is the tree representation of a call in a Path context.
//
// Example:
// 		g.V(g.IRI("alice"))
// 		is represented as
// 		&Vertex{ Values: []quad.Value{quad.IRI("alice")} }
//
// 		g.V().out(g.IRI("likes"))
// 		is represented as
// 		&Out{ Properties: []quad.Value{quad.IRI("likes")}, From: &Vertex{} }
type Step interface {
	RegistryItem
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

// EntityIdentifier is an interface to be used where a single entity identifier is expected.
type EntityIdentifier interface {
	BuildIdentifier() (quad.Value, error)
}

// EntityIRI is an entity IRI.
type EntityIRI quad.IRI

// BuildIdentifier implements EntityIdentifier
func (i EntityIRI) BuildIdentifier() (quad.Value, error) {
	return quad.IRI(i), nil
}

// EntityBNode is an entity BNode.
type EntityBNode quad.BNode

// BuildIdentifier implements EntityIdentifier
func (i EntityBNode) BuildIdentifier() (quad.Value, error) {
	return quad.BNode(i), nil
}

// EntityIdentifierString is an entity IRI or BNode strings.
type EntityIdentifierString string

// BuildIdentifier implements EntityIdentifier
func (i EntityIdentifierString) BuildIdentifier() (quad.Value, error) {
	return parseIdentifier(string(i))
}

var _ IteratorStep = (*Entity)(nil)
var _ PathStep = (*Entity)(nil)

// Entity corresponds to g.Entity().
type Entity struct {
	Identifier EntityIdentifier `json:"identifier"`
}

// Type implements Step.
func (s *Entity) Type() quad.IRI {
	return Prefix + "Entity"
}

// Description implements Step.
func (s *Entity) Description() string {
	return "resolves to the object matching given identifier in the graph."
}

// BuildIterator implements IteratorStep.
func (s *Entity) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Entity) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	identifier, err := s.Identifier.BuildIdentifier()
	if err != nil {
		return nil, err
	}
	return path.StartPath(qs, identifier), nil
}

var _ IteratorStep = (*Entities)(nil)
var _ PathStep = (*Entities)(nil)

// Entities corresponds to g.Entities().
type Entities struct {
	Identifiers []EntityIdentifier `json:"identifiers"`
}

// Type implements Step.
func (s *Entities) Type() quad.IRI {
	return Prefix + "Entities"
}

// Description implements Step.
func (s *Entities) Description() string {
	return "resolves to all the existing objects in the graph. If provided with identifiers resolves to a sublist of all the existing identifiers in the graph."
}

// BuildIterator implements IteratorStep.
func (s *Entities) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Entities) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	var values []quad.Value
	for _, identifier := range s.Identifiers {
		value, err := identifier.BuildIdentifier()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	// TODO(iddan): Construct a path that only match entities
	return path.StartPath(qs, values...), nil
}

var _ IteratorStep = (*Vertex)(nil)
var _ PathStep = (*Vertex)(nil)

// Vertex corresponds to g.Vertex() and g.V().
type Vertex struct {
	Values Values `json:"values"`
}

// Type implements Step.
func (s *Vertex) Type() quad.IRI {
	return Prefix + "Vertex"
}

// Description implements Step.
func (s *Vertex) Description() string {
	return "resolves to all the existing objects and primitive values in the graph. If provided with values resolves to a sublist of all the existing values in the graph."
}

// BuildIterator implements IteratorStep.
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
	return Prefix + "Placeholder"
}

// Description implements Step.
func (s *Placeholder) Description() string {
	return "is like Vertex but resolves to the values in the context it is placed in. It should only be used where a PathStep is expected and can't be resolved on its own."
}

// BuildPath implements PathStep.
func (s *Placeholder) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	return path.StartMorphism(), nil
}

var _ IteratorStep = (*Visit)(nil)
var _ PathStep = (*Visit)(nil)

// Visit corresponds to .view().
type Visit struct {
	From       PathStep     `json:"from"`
	Properties PropertyPath `json:"properties"`
}

// Type implements Step.
func (s *Visit) Type() quad.IRI {
	return Prefix + "Visit"
}

// Description implements Step.
func (s *Visit) Description() string {
	return "resolves to the values of the given property or properties in via of the current objects. If via is a path it's resolved values will be used as properties."
}

// BuildIterator implements IteratorStep.
func (s *Visit) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Visit) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Properties.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Out(viaPath), nil
}

var _ IteratorStep = (*Out)(nil)
var _ PathStep = (*Out)(nil)

// Out is an alias for View.
type Out struct {
	Visit
}

// Type implements Step.
func (s *Out) Type() quad.IRI {
	return Prefix + "Out"
}

// Description implements Step.
func (s *Out) Description() string {
	return "aliases for View"
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
	return Prefix + "As"
}

// Description implements Step.
func (s *As) Description() string {
	return "assigns the resolved values of the from step to a given name. The name can be used with the Select and Documents steps to retrieve the values or to return to the values in further steps with the Back step. It resolves to the values of the from step."
}

// BuildIterator implements IteratorStep.
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
	return Prefix + "Intersect"
}

// Description implements Step.
func (s *Intersect) Description() string {
	return "resolves to all the same values resolved by the from step and the provided steps."
}

// BuildIterator implements IteratorStep.
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
	From   PathStep `json:"from"`
	Values Values   `json:"values"`
}

// Type implements Step.
func (s *Is) Type() quad.IRI {
	return Prefix + "Is"
}

// Description implements Step.
func (s *Is) Description() string {
	return "resolves to all the values resolved by the from step which are included in provided values."
}

// BuildIterator implements IteratorStep.
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
	return Prefix + "Back"
}

// Description implements Step.
func (s *Back) Description() string {
	return "resolves to the values of the previous the step or the values assigned to name in a former step."
}

// BuildIterator implements IteratorStep.
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

var _ IteratorStep = (*Both)(nil)
var _ PathStep = (*Both)(nil)

// Both corresponds to .both().
type Both struct {
	From       PathStep     `json:"from"`
	Properties PropertyPath `json:"properties"`
}

// Type implements Step.
func (s *Both) Type() quad.IRI {
	return Prefix + "Both"
}

// Description implements Step.
func (s *Both) Description() string {
	return "is like View but resolves to both the object values and references to the values of the given properties in via. It is the equivalent for the Union of View and ViewReverse of the same property."
}

// BuildIterator implements IteratorStep.
func (s *Both) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Both) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Properties.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Both(viaPath), nil
}

var _ IteratorStep = (*Count)(nil)
var _ PathStep = (*Count)(nil)

// Count corresponds to .count().
type Count struct {
	From PathStep `json:"from"`
}

// Type implements Step.
func (s *Count) Type() quad.IRI {
	return Prefix + "Count"
}

// Description implements Step.
func (s *Count) Description() string {
	return "resolves to the number of the resolved values of the from step"
}

// BuildIterator implements IteratorStep.
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

// Difference corresponds to .difference().
type Difference struct {
	From  PathStep   `json:"from"`
	Steps []PathStep `json:"steps"`
}

// Type implements Step.
func (s *Difference) Type() quad.IRI {
	return Prefix + "Difference"
}

// Description implements Step.
func (s *Difference) Description() string {
	return "resolves to all the values resolved by the from step different then the values resolved by the provided steps. Caution: it might be slow to execute."
}

// BuildIterator implements IteratorStep.
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
	return Prefix + "Filter"
}

// Description implements Step.
func (s *Filter) Description() string {
	return "applies constraints to a set of nodes. Can be used to filter values by range or match strings."
}

// BuildIterator implements IteratorStep.
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
	return Prefix + "Follow"
}

// Description implements Step.
func (s *Follow) Description() string {
	return "is the way to use a path prepared with Morphism. Applies the path chain on the morphism object to the current path. Starts as if at the g.M() and follows through the morphism path."
}

// BuildIterator implements IteratorStep.
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
	return Prefix + "FollowReverse"
}

// Description implements Step.
func (s *FollowReverse) Description() string {
	return "is the same as follow but follows the chain in the reverse direction. Flips View and ViewReverse where appropriate, the net result being a virtual predicate followed in the reverse direction. Starts at the end of the morphism and follows it backwards (with appropriate flipped directions) to the g.M() location."
}

// BuildIterator implements IteratorStep.
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
	From     PathStep     `json:"from"`
	Property PropertyPath `json:"property"`
	Values   Values       `json:"values"`
}

// Type implements Step.
func (s *Has) Type() quad.IRI {
	return Prefix + "Has"
}

// Description implements Step.
func (s *Has) Description() string {
	return "filters all paths which are, at this point, on the subject for the given predicate and object, but do not follow the path, merely filter the possible paths. Usually useful for starting with all nodes, or limiting to a subset depending on some predicate/value pair."
}

// BuildIterator implements IteratorStep.
func (s *Has) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *Has) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Property.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Has(viaPath, s.Values...), nil
}

var _ IteratorStep = (*HasReverse)(nil)
var _ PathStep = (*HasReverse)(nil)

// HasReverse corresponds to .hasR().
type HasReverse struct {
	From     PathStep     `json:"from"`
	Property PropertyPath `json:"property"`
	Values   Values       `json:"values"`
}

// Type implements Step.
func (s *HasReverse) Type() quad.IRI {
	return Prefix + "HasReverse"
}

// Description implements Step.
func (s *HasReverse) Description() string {
	return "is the same as Has, but sets constraint in reverse direction."
}

// BuildIterator implements IteratorStep.
func (s *HasReverse) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *HasReverse) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Property.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.HasReverse(viaPath, s.Values...), nil
}

var _ IteratorStep = (*VisitReverse)(nil)
var _ PathStep = (*VisitReverse)(nil)

// VisitReverse corresponds to .viewReverse().
type VisitReverse struct {
	From       PathStep     `json:"from"`
	Properties PropertyPath `json:"properties"`
}

// Type implements Step.
func (s *VisitReverse) Type() quad.IRI {
	return Prefix + "VisitReverse"
}

// Description implements Step.
func (s *VisitReverse) Description() string {
	return "is the inverse of View. Starting with the nodes in `path` on the object, follow the quads with predicates defined by `predicatePath` to their subjects."
}

// BuildIterator implements IteratorStep.
func (s *VisitReverse) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements PathStep.
func (s *VisitReverse) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	viaPath, err := s.Properties.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.In(viaPath), nil
}

var _ IteratorStep = (*In)(nil)
var _ PathStep = (*In)(nil)

// In is an alias for ViewReverse.
type In struct {
	VisitReverse
}

// Type implements Step.
func (s *In) Type() quad.IRI {
	return Prefix + "In"
}

// Description implements Step.
func (s *In) Description() string {
	return "aliases for ViewReverse"
}

var _ IteratorStep = (*ReversePropertyNames)(nil)
var _ PathStep = (*ReversePropertyNames)(nil)

// ReversePropertyNames corresponds to .reversePropertyNames().
type ReversePropertyNames struct {
	From PathStep `json:"from"`
}

// Type implements Step.
func (s *ReversePropertyNames) Type() quad.IRI {
	return Prefix + "ReversePropertyNames"
}

// Description implements Step.
func (s *ReversePropertyNames) Description() string {
	return "gets the list of predicates that are pointing in to a node."
}

// BuildIterator implements IteratorStep.
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
	return Prefix + "Labels"
}

// Description implements Step.
func (s *Labels) Description() string {
	return "gets the list of inbound and outbound quad labels"
}

// BuildIterator implements IteratorStep.
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
	return Prefix + "Limit"
}

// Description implements Step.
func (s *Limit) Description() string {
	return "limits a number of nodes for current path."
}

// BuildIterator implements IteratorStep.
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
	return Prefix + "PropertyNames"
}

// Description implements Step.
func (s *PropertyNames) Description() string {
	return "gets the list of predicates that are pointing out from a node."
}

// BuildIterator implements IteratorStep.
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
	From PathStep `json:"from"`
	// TODO(iddan): Use PropertyPath
	Names []quad.IRI `json:"names"`
}

// Type implements Step.
func (s *Properties) Type() quad.IRI {
	return Prefix + "Properties"
}

// Description implements Step.
func (s *Properties) Description() string {
	return "adds tags for all properties of the current entity"
}

// BuildIterator implements IteratorStep.
// TODO(iddan): Default tag to Via.
func (s *Properties) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
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
	return Prefix + "ReversePropertyNamesAs"
}

// Description implements Step.
func (s *ReversePropertyNamesAs) Description() string {
	return "tags the list of predicates that are pointing in to a node."
}

// BuildIterator implements IteratorStep.
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
	return Prefix + "PropertyNamesAs"
}

// Description implements Step.
func (s *PropertyNamesAs) Description() string {
	return "tags the list of predicates that are pointing out from a node."
}

// BuildIterator implements IteratorStep.
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
	From PathStep `json:"from"`
	// TODO(iddan): Use property path
	Names []quad.IRI `json:"names"`
}

// Type implements Step.
func (s *ReverseProperties) Type() quad.IRI {
	return Prefix + "ReverseProperties"
}

// Description implements Step.
func (s *ReverseProperties) Description() string {
	return "gets all the properties the current entity / value is referenced at"
}

// BuildIterator implements IteratorStep.
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
	return Prefix + "Skip"
}

// Description implements Step.
func (s *Skip) Description() string {
	return "skips a number of nodes for current path."
}

// BuildIterator implements IteratorStep.
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
	return Prefix + "Union"
}

// Description implements Step.
func (s *Union) Description() string {
	return "returns the combined paths of the two queries. Notice that it's per-path, not per-node. Once again, if multiple paths reach the same destination, they might have had different ways of getting there (and different tags)."
}

// BuildIterator implements IteratorStep.
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
	return Prefix + "Unique"
}

// Description implements Step.
func (s *Unique) Description() string {
	return "removes duplicate values from the path."
}

// BuildIterator implements IteratorStep.
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
	return Prefix + "Order"
}

// Description implements Step.
func (s *Order) Description() string {
	return "sorts the results in ascending order according to the current entity / value"
}

// BuildIterator implements IteratorStep.
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
	return Prefix + "Optional"
}

// Description implements Step.
func (s *Optional) Description() string {
	return "attempts to follow the given path from the current entity / value, if fails the entity / value will still be kept in the results"
}

// BuildIterator implements IteratorStep.
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

var _ IteratorStep = (*Where)(nil)
var _ PathStep = (*Where)(nil)

// Where corresponds to .where().
type Where struct {
	From  PathStep   `json:"from"`
	Steps []PathStep `json:"steps"`
}

// Type implements Step.
func (s *Where) Type() quad.IRI {
	return Prefix + "Where"
}

// Description implements Step.
func (s *Where) Description() string {
	return "applies each provided step in steps in isolation on from"
}

// BuildIterator implements IteratorStep.
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

var _ IteratorStep = (*LessThan)(nil)
var _ PathStep = (*LessThan)(nil)

// LessThan corresponds to lt().
type LessThan struct {
	From  PathStep   `json:"from"`
	Value quad.Value `json:"value"`
}

// Type implements Step.
func (s *LessThan) Type() quad.IRI {
	return Prefix + "LessThan"
}

// Description implements Step.
func (s *LessThan) Description() string {
	return "Less than filters out values that are not less than given value"
}

// BuildIterator implements IteratorStep.
func (s *LessThan) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements Step.
func (s *LessThan) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Filter(iterator.CompareLT, s.Value), nil
}

var _ IteratorStep = (*LessThanEquals)(nil)
var _ PathStep = (*LessThanEquals)(nil)

// LessThanEquals corresponds to lte().
type LessThanEquals struct {
	From  PathStep   `json:"from"`
	Value quad.Value `json:"value"`
}

// Type implements Step.
func (s *LessThanEquals) Type() quad.IRI {
	return Prefix + "LessThanEquals"
}

// Description implements Step.
func (s *LessThanEquals) Description() string {
	return "Less than equals filters out values that are not less than or equal given value"
}

// BuildIterator implements Step.
func (s *LessThanEquals) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements Step.
func (s *LessThanEquals) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Filter(iterator.CompareLTE, s.Value), nil
}

var _ IteratorStep = (*GreaterThan)(nil)
var _ PathStep = (*GreaterThan)(nil)

// GreaterThan corresponds to gt().
type GreaterThan struct {
	From  PathStep   `json:"from"`
	Value quad.Value `json:"value"`
}

// Type implements Step.
func (s *GreaterThan) Type() quad.IRI {
	return Prefix + "GreaterThan"
}

// Description implements Step.
func (s *GreaterThan) Description() string {
	return "Greater than equals filters out values that are not greater than given value"
}

// BuildIterator implements Step.
func (s *GreaterThan) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements Step.
func (s *GreaterThan) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Filter(iterator.CompareGT, s.Value), nil
}

var _ IteratorStep = (*GreaterThanEquals)(nil)
var _ PathStep = (*GreaterThanEquals)(nil)

// GreaterThanEquals corresponds to gte().
type GreaterThanEquals struct {
	From  PathStep   `json:"from"`
	Value quad.Value `json:"value"`
}

// Type implements Step.
func (s *GreaterThanEquals) Type() quad.IRI {
	return Prefix + "GreaterThanEquals"
}

// Description implements Step.
func (s *GreaterThanEquals) Description() string {
	return "Greater than equals filters out values that are not greater than or equal given value"
}

// BuildIterator implements Step.
func (s *GreaterThanEquals) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return NewValueIteratorFromPathStep(s, qs)
}

// BuildPath implements Step.
func (s *GreaterThanEquals) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return fromPath.Filter(iterator.CompareGTE, s.Value), nil
}
