package linkedql

import (
	"regexp"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/cayley/query/shape"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
)

const namespace = "http://cayley.io/linkedql#"
const prefix = "linkedql:"

func init() {
	voc.Register(voc.Namespace{Full: namespace, Prefix: prefix})
	Register(&Vertex{})
	Register(&Out{})
	Register(&As{})
	Register(&TagArray{})
	Register(&Value{})
	Register(&Intersect{})
	Register(&Is{})
	Register(&Back{})
	Register(&Both{})
	Register(&Count{})
	Register(&Except{})
	Register(&LessThan{})
	Register(&LessThanEquals{})
	Register(&GreaterThan{})
	Register(&GreaterThanEquals{})
	Register(&RegExp{})
	Register(&Like{})
	Register(&Filter{})
	Register(&Follow{})
	Register(&FollowReverse{})
	Register(&Has{})
	Register(&HasReverse{})
	Register(&In{})
	Register(&InPredicates{})
	Register(&LabelContext{})
	Register(&Labels{})
	Register(&Limit{})
	Register(&OutPredicates{})
	Register(&Save{})
	Register(&SaveInPredicates{})
	Register(&SaveOptional{})
	Register(&SaveOptionalReverse{})
	Register(&SaveOutPredicates{})
	Register(&SaveReverse{})
	Register(&Skip{})
	Register(&Union{})
	Register(&Unique{})
	Register(&Order{})
}

// Vertex corresponds to g.Vertex() and g.V()
type Vertex struct {
	Values []quad.Value `json:"values"`
}

// Type implements Step
func (s *Vertex) Type() quad.IRI {
	return prefix + "Vertex"
}

// BuildIterator implements Step
func (s *Vertex) BuildIterator(qs graph.QuadStore) query.Iterator {
	path := path.StartPath(qs, s.Values...)
	return NewValueIterator(path, qs)
}

// Morphism corresponds to g.Morphism() and g.M()
type Morphism struct {
}

// Type implements Step
func (s *Morphism) Type() quad.IRI {
	return prefix + "Morphism"
}

// BuildIterator implements Step
func (s *Morphism) BuildIterator(qs graph.QuadStore) query.Iterator {
	panic("Not implemented")
}

// Out corresponds to .out()
type Out struct {
	From Step     `json:"from"`
	Via  Step     `json:"via"`
	Tags []string `json:"tags"`
}

// Type implements Step
func (s *Out) Type() quad.IRI {
	return prefix + "Out"
}

// BuildIterator implements Step
func (s *Out) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Out must be called from ValueIterator")
	}
	viaIt, ok := s.Via.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Out must be called with ValueIterator via")
	}
	path := fromIt.path.OutWithTags(s.Tags, viaIt.path)
	return NewValueIterator(path, qs)
}

// As corresponds to .tag()
type As struct {
	From Step     `json:"from"`
	Tags []string `json:"tags"`
}

// Type implements Step
func (s *As) Type() quad.IRI {
	return prefix + "As"
}

// BuildIterator implements Step
func (s *As) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("As must be called from ValueIterator")
	}
	path := fromIt.path.Tag(s.Tags...)
	return NewValueIterator(path, qs)
}

// TagArray corresponds to .tagArray()
type TagArray struct {
	From Step `json:"from"`
}

// Type implements Step
func (s *TagArray) Type() quad.IRI {
	return prefix + "TagArray"
}

// BuildIterator implements Step
func (s *TagArray) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("TagArray must be called from ValueIterator")
	}
	return &TagArrayIterator{fromIt}
}

// Value corresponds to .value()
type Value struct {
	From Step `json:"from"`
}

// Type implements Step
func (s *Value) Type() quad.IRI {
	return prefix + "Value"
}

// BuildIterator implements Step
func (s *Value) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Value must be called from ValueIterator")
	}
	// TODO(@iddan): support non iterators for query result
	return fromIt
}

// Intersect represents .intersect() and .and()
type Intersect struct {
	From        Step `json:"from"`
	Intersectee Step `json:"intersectee"`
}

// Type implements Step
func (s *Intersect) Type() quad.IRI {
	return prefix + "Intersect"
}

// BuildIterator implements Step
func (s *Intersect) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Intersect must be called from ValueIterator")
	}
	intersecteeIt, ok := s.Intersectee.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Intersect must be called with ValueIterator intersectee")
	}
	return NewValueIterator(fromIt.path.And(intersecteeIt.path), qs)
}

// Is corresponds to .back()
type Is struct {
	From   Step         `json:"from"`
	Values []quad.Value `json:"values"`
}

// Type implements Step
func (s *Is) Type() quad.IRI {
	return prefix + "Is"
}

// BuildIterator implements Step
func (s *Is) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Is must be called from ValueIterator")
	}
	return NewValueIterator(fromIt.path.Is(s.Values...), qs)
}

// Back corresponds to .back()
type Back struct {
	From Step   `json:"from"`
	Tag  string `json:"tag"`
}

// Type implements Step
func (s *Back) Type() quad.IRI {
	return prefix + "Back"
}

// BuildIterator implements Step
func (s *Back) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Back must be called from ValueIterator")
	}
	return NewValueIterator(fromIt.path.Back(s.Tag), qs)
}

// Both corresponds to .both()
type Both struct {
	From Step     `json:"from"`
	Via  Step     `json:"via"`
	Tags []string `json:"tags"`
}

// Type implements Step
func (s *Both) Type() quad.IRI {
	return prefix + "Both"
}

// BuildIterator implements Step
func (s *Both) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Both must be called from ValueIterator")
	}
	viaIt, ok := s.Via.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Both must be called with ValueIterator via")
	}
	return NewValueIterator(fromIt.path.BothWithTags(s.Tags, viaIt.path), qs)
}

// Count corresponds to .count()
type Count struct {
	From Step `json:"from"`
}

// Type implements Step
func (s *Count) Type() quad.IRI {
	return prefix + "Count"
}

// BuildIterator implements Step
func (s *Count) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Count must be called from ValueIterator")
	}
	return NewValueIterator(fromIt.path.Count(), qs)
}

// Except corresponds to .except() and .difference()
type Except struct {
	From     Step `json:"from"`
	Excepted Step `json:"excepted"`
}

// Type implements Step
func (s *Except) Type() quad.IRI {
	return prefix + "Except"
}

// BuildIterator implements Step
func (s *Except) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Except must be called from ValueIterator")
	}
	exceptedIt, ok := s.Excepted.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Except must be called with ValueIterator excepted")
	}
	return NewValueIterator(fromIt.path.Except(exceptedIt.path), qs)
}

// LessThan corresponds to lt()
type LessThan struct {
	Value quad.Value `json:"value"`
}

// Type implements Step
func (s *LessThan) Type() quad.IRI {
	return prefix + "LessThan"
}

// BuildIterator implements Step
func (s *LessThan) BuildIterator(qs graph.QuadStore) query.Iterator {
	panic("Can't BuildIterator for " + s.Type())
}

// LessThanEquals corresponds to lte()
type LessThanEquals struct {
	Value quad.Value `json:"value"`
}

// Type implements Step
func (s *LessThanEquals) Type() quad.IRI {
	return prefix + "LessThanEquals"
}

// BuildIterator implements Step
func (s *LessThanEquals) BuildIterator(qs graph.QuadStore) query.Iterator {
	panic("Can't BuildIterator for " + s.Type())
}

// GreaterThan corresponds to gt()
type GreaterThan struct {
	Value quad.Value `json:"value"`
}

// Type implements Step
func (s *GreaterThan) Type() quad.IRI {
	return prefix + "GreaterThan"
}

// BuildIterator implements Step
func (s *GreaterThan) BuildIterator(qs graph.QuadStore) query.Iterator {
	panic("Can't BuildIterator for " + s.Type())
}

// GreaterThanEquals corresponds to gte()
type GreaterThanEquals struct {
	Value quad.Value `json:"value"`
}

// Type implements Step
func (s *GreaterThanEquals) Type() quad.IRI {
	return prefix + "GreaterThanEquals"
}

// BuildIterator implements Step
func (s *GreaterThanEquals) BuildIterator(qs graph.QuadStore) query.Iterator {
	panic("Can't BuildIterator for " + s.Type())
}

// RegExp corresponds to regex()
type RegExp struct {
	Expression  string `json:"expression"`
	IncludeIRIs bool   `json:"includeIRIs"`
}

// Type implements Step
func (s *RegExp) Type() quad.IRI {
	return prefix + "RegExp"
}

// BuildIterator implements Step
func (s *RegExp) BuildIterator(qs graph.QuadStore) query.Iterator {
	panic("Can't BuildIterator for " + s.Type())
}

// Like corresponds to like()
type Like struct {
	Pattern string `json:"pattern"`
}

// Type implements Step
func (s *Like) Type() quad.IRI {
	return prefix + "Like"
}

// BuildIterator implements Step
func (s *Like) BuildIterator(qs graph.QuadStore) query.Iterator {
	panic("Can't BuildIterator for " + s.Type())
}

// Filter corresponds to filter()
type Filter struct {
	From   Step `json:"from"`
	Filter Step `json:"filter"`
}

// Type implements Step
func (s *Filter) Type() quad.IRI {
	return prefix + "Filter"
}

// BuildIterator implements Step
func (s *Filter) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Except must be called from ValueIterator")
	}
	switch filter := s.Filter.(type) {
	case *LessThan:
		return NewValueIterator(fromIt.path.Filter(iterator.Operator(iterator.CompareLT), filter.Value), qs)
	case *LessThanEquals:
		return NewValueIterator(fromIt.path.Filter(iterator.Operator(iterator.CompareLTE), filter.Value), qs)
	case *GreaterThan:
		return NewValueIterator(fromIt.path.Filter(iterator.Operator(iterator.CompareGT), filter.Value), qs)
	case *GreaterThanEquals:
		return NewValueIterator(fromIt.path.Filter(iterator.Operator(iterator.CompareGTE), filter.Value), qs)
	case *RegExp:
		expression, err := regexp.Compile(string(filter.Expression))
		if err != nil {
			panic("Invalid RegExp")
		}
		if filter.IncludeIRIs {
			return NewValueIterator(fromIt.path.RegexWithRefs(expression), qs)
		}
		return NewValueIterator(fromIt.path.RegexWithRefs(expression), qs)
	case *Like:
		return NewValueIterator(fromIt.path.Filters(shape.Wildcard{Pattern: filter.Pattern}), qs)
	default:
		panic("Filter is not recognized")
	}
}

// Follow corresponds to .follow()
type Follow struct {
	From     Step `json:"from"`
	Followed Step `json:"followed"`
}

// Type implements Step
func (s *Follow) Type() quad.IRI {
	return prefix + "Follow"
}

// BuildIterator implements Step
func (s *Follow) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Follow must be called from ValueIterator")
	}
	followedIt, ok := s.Followed.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Follow must be called with ValueIterator followed")
	}
	return NewValueIterator(fromIt.path.Follow(followedIt.path), qs)
}

// FollowReverse corresponds to .followR()
type FollowReverse struct {
	From     Step `json:"from"`
	Followed Step `json:"followed"`
}

// Type implements Step
func (s *FollowReverse) Type() quad.IRI {
	return prefix + "FollowReverse"
}

// BuildIterator implements Step
func (s *FollowReverse) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("FollowR must be called from ValueIterator")
	}
	followedIt, ok := s.Followed.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("FollowR must be called with ValueIterator followed")
	}
	return NewValueIterator(fromIt.path.FollowReverse(followedIt.path), qs)
}

// FollowRecursive corresponds to .followRecursive()
type FollowRecursive struct {
	From     Step `json:"from"`
	Followed Step `json:"followed"`
}

// Type implements Step
func (s *FollowRecursive) Type() quad.IRI {
	return prefix + "FollowRecursive"
}

// BuildIterator implements Step
func (s *FollowRecursive) BuildIterator(qs graph.QuadStore) query.Iterator {
	panic("Not Implemented")
}

// Has corresponds to .has()
type Has struct {
	From   Step         `json:"from"`
	Via    Step         `json:"via"`
	Values []quad.Value `json:"values"`
}

// Type implements Step
func (s *Has) Type() quad.IRI {
	return prefix + "Has"
}

// BuildIterator implements Step
func (s *Has) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Has must be called from ValueIterator")
	}
	viaIt, ok := s.Via.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Has must be called with ValueIterator via")
	}
	return NewValueIterator(fromIt.path.Has(viaIt.path, s.Values...), qs)
}

// HasReverse corresponds to .hasR()
type HasReverse struct {
	From   Step         `json:"from"`
	Via    Step         `json:"via"`
	Values []quad.Value `json:"values"`
}

// Type implements Step
func (s *HasReverse) Type() quad.IRI {
	return prefix + "HasReverse"
}

// BuildIterator implements Step
func (s *HasReverse) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Has must be called from ValueIterator")
	}
	viaIt, ok := s.Via.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Has must be called with ValueIterator via")
	}
	return NewValueIterator(fromIt.path.HasReverse(viaIt.path, s.Values...), qs)
}

// In corresponds to .in()
type In struct {
	From Step     `json:"from"`
	Via  Step     `json:"via"`
	Tags []string `json:"tags"`
}

// Type implements Step
func (s *In) Type() quad.IRI {
	return prefix + "In"
}

// BuildIterator implements Step
func (s *In) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("In must be called from ValueIterator")
	}
	viaIt, ok := s.Via.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("In must be called with ValueIterator via")
	}
	return NewValueIterator(fromIt.path.InWithTags(s.Tags, viaIt.path), qs)
}

// InPredicates corresponds to .inPredicates()
type InPredicates struct {
	From Step `json:"from"`
}

// Type implements Step
func (s *InPredicates) Type() quad.IRI {
	return prefix + "InPredicates"
}

// BuildIterator implements Step
func (s *InPredicates) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("InPredicates must be called from ValueIterator")
	}
	return NewValueIterator(fromIt.path.InPredicates(), qs)
}

// LabelContext corresponds to .labelContext()
type LabelContext struct {
	From Step `json:"from"`
	// TODO(@iddan): Via
}

// Type implements Step
func (s *LabelContext) Type() quad.IRI {
	return prefix + "LabelContext"
}

// BuildIterator implements Step
func (s *LabelContext) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("LabelContext must be called from ValueIterator")
	}
	return NewValueIterator(fromIt.path.LabelContext(), qs)
}

// Labels corresponds to .labels()
type Labels struct {
	From Step `json:"from"`
}

// Type implements Step
func (s *Labels) Type() quad.IRI {
	return prefix + "Labels"
}

// BuildIterator implements Step
func (s *Labels) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Labels must be called from ValueIterator")
	}
	return NewValueIterator(fromIt.path.Labels(), qs)
}

// Limit corresponds to .limit()
type Limit struct {
	From  Step  `json:"from"`
	Limit int64 `json:"limit"`
}

// Type implements Step
func (s *Limit) Type() quad.IRI {
	return prefix + "Limit"
}

// BuildIterator implements Step
func (s *Limit) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Limit must be called from ValueIterator")
	}
	return NewValueIterator(fromIt.path.Limit(s.Limit), qs)
}

// OutPredicates corresponds to .outPredicates()
type OutPredicates struct {
	From Step `json:"from"`
}

// Type implements Step
func (s *OutPredicates) Type() quad.IRI {
	return prefix + "OutPredicates"
}

// BuildIterator implements Step
func (s *OutPredicates) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("OutPredicates must be called from ValueIterator")
	}
	return NewValueIterator(fromIt.path.OutPredicates(), qs)
}

// Save corresponds to .save()
type Save struct {
	From Step   `json:"from"`
	Via  Step   `json:"via"`
	Tag  string `json:"tag"`
}

// Type implements Step
func (s *Save) Type() quad.IRI {
	return prefix + "Save"
}

// BuildIterator implements Step
// TODO(iddan): Default tag to Via
func (s *Save) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Save must be called from ValueIterator")
	}
	viaIt, ok := s.Via.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Save must be called with ValueIterator via")
	}
	return NewValueIterator(fromIt.path.Save(viaIt.path, s.Tag), qs)
}

// SaveInPredicates corresponds to .saveInPredicates()
type SaveInPredicates struct {
	From Step   `json:"from"`
	Tag  string `json:"tag"`
}

// Type implements Step
func (s *SaveInPredicates) Type() quad.IRI {
	return prefix + "SaveInPredicates"
}

// BuildIterator implements Step
func (s *SaveInPredicates) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("SaveInPredicates must be called from ValueIterator")
	}
	return NewValueIterator(fromIt.path.SavePredicates(true, s.Tag), qs)
}

// SaveOptional corresponds to .saveOpt()
type SaveOptional struct {
	From Step   `json:"from"`
	Via  Step   `json:"via"`
	Tag  string `json:"tag"`
}

// Type implements Step
func (s *SaveOptional) Type() quad.IRI {
	return prefix + "SaveOptional"
}

// BuildIterator implements Step
func (s *SaveOptional) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("SaveOptional must be called from ValueIterator")
	}
	viaIt, ok := s.Via.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("SaveOptional must be called with ValueIterator via")
	}
	return NewValueIterator(fromIt.path.SaveOptional(viaIt.path, s.Tag), qs)
}

// SaveOptionalReverse corresponds to .saveOptR()
type SaveOptionalReverse struct {
	From Step   `json:"from"`
	Via  Step   `json:"via"`
	Tag  string `json:"tag"`
}

// Type implements Step
func (s *SaveOptionalReverse) Type() quad.IRI {
	return prefix + "SaveOptionalReverse"
}

// BuildIterator implements Step
func (s *SaveOptionalReverse) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("SaveOptionalReverse must be called from ValueIterator")
	}
	viaIt, ok := s.Via.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("SaveOptionalReverse must be called with ValueIterator via")
	}
	return NewValueIterator(fromIt.path.SaveOptionalReverse(viaIt.path, s.Tag), qs)
}

// SaveOutPredicates corresponds to .saveOutPredicates()
type SaveOutPredicates struct {
	From Step   `json:"from"`
	Tag  string `json:"tag"`
}

// Type implements Step
func (s *SaveOutPredicates) Type() quad.IRI {
	return prefix + "SaveOutPredicates"
}

// BuildIterator implements Step
func (s *SaveOutPredicates) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("SaveOutPredicates must be called from ValueIterator")
	}
	return NewValueIterator(fromIt.path.SavePredicates(false, s.Tag), qs)
}

// SaveReverse corresponds to .saveR()
type SaveReverse struct {
	From Step   `json:"from"`
	Via  Step   `json:"via"`
	Tag  string `json:"tag"`
}

// Type implements Step
func (s *SaveReverse) Type() quad.IRI {
	return prefix + "SaveReverse"
}

// BuildIterator implements Step
func (s *SaveReverse) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("SaveReverse must be called from ValueIterator")
	}
	viaIt, ok := s.Via.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("SaveReverse must be called with ValueIterator via")
	}
	return NewValueIterator(fromIt.path.SaveReverse(viaIt.path, s.Tag), qs)
}

// Skip corresponds to .skip()
type Skip struct {
	From   Step  `json:"from"`
	Offset int64 `json:"offset"`
}

// Type implements Step
func (s *Skip) Type() quad.IRI {
	return prefix + "Skip"
}

// BuildIterator implements Step
func (s *Skip) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("SaveReverse must be called from ValueIterator")
	}
	return NewValueIterator(fromIt.path.Skip(s.Offset), qs)
}

// Union corresponds to .union() and .or()
type Union struct {
	From      Step `json:"from"`
	Unionized Step `json:"unionized"`
}

// Type implements Step
func (s *Union) Type() quad.IRI {
	return prefix + "Union"
}

// BuildIterator implements Step
func (s *Union) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Union must be called from ValueIterator")
	}
	unionizedIt, ok := s.Unionized.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Union must be called with ValueIterator Unionized")
	}
	return NewValueIterator(fromIt.path.Or(unionizedIt.path), qs)
}

// Unique corresponds to .unique()
type Unique struct {
	From Step `json:"from"`
}

// Type implements Step
func (s *Unique) Type() quad.IRI {
	return prefix + "Unique"
}

// BuildIterator implements Step
func (s *Unique) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Unique must be called from ValueIterator")
	}
	return NewValueIterator(fromIt.path.Unique(), qs)
}

// Order corresponds to .order()
type Order struct {
	From Step `json:"from"`
}

// Type implements Step
func (s *Order) Type() quad.IRI {
	return prefix + "Order"
}

// BuildIterator implements Step
func (s *Order) BuildIterator(qs graph.QuadStore) query.Iterator {
	fromIt, ok := s.From.BuildIterator(qs).(*ValueIterator)
	if !ok {
		panic("Order must be called from ValueIterator")
	}
	return NewValueIterator(fromIt.path.Order(), qs)
}
