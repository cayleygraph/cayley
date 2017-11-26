package shape

import (
	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
	"regexp"
)

// Shape represent a query tree shape.
type Shape interface {
	// BuildIterator constructs an iterator tree from a given shapes and binds it to QuadStore.
	BuildIterator(qs graph.QuadStore) graph.Iterator
	// Optimize runs an optimization pass over a query shape.
	//
	// It returns a bool that indicates if shape was replaced and should always return a copy of shape in this case.
	// In case no optimizations were made, it returns the same unmodified shape.
	//
	// If Optimizer is specified, it will be used instead of default optimizations.
	Optimize(r Optimizer) (Shape, bool)
}

type Optimizer interface {
	OptimizeShape(s Shape) (Shape, bool)
}

type resolveValues struct {
	qs graph.QuadStore
}

func (r resolveValues) OptimizeShape(s Shape) (Shape, bool) {
	if l, ok := s.(Lookup); ok {
		return l.resolve(r.qs), true
	}
	return s, false
}

func Optimize(s Shape, qs graph.QuadStore) (Shape, bool) {
	if s == nil {
		return nil, false
	}
	var opt bool
	if qs != nil {
		// resolve all lookups earlier
		s, opt = s.Optimize(resolveValues{qs: qs})
	}
	if s == nil {
		return Null{}, true
	}
	// generic optimizations
	var opt1 bool
	s, opt1 = s.Optimize(nil)
	if s == nil {
		return Null{}, true
	}
	opt = opt || opt1
	// apply quadstore-specific optimizations
	if so, ok := qs.(Optimizer); ok && s != nil {
		var opt2 bool
		s, opt2 = s.Optimize(so)
		opt = opt || opt2
	}
	if s == nil {
		return Null{}, true
	}
	return s, opt
}

// InternalQuad is an internal representation of quad index in QuadStore.
type InternalQuad struct {
	Subject   graph.Value
	Predicate graph.Value
	Object    graph.Value
	Label     graph.Value
}

// Get returns a specified direction of the quad.
func (q InternalQuad) Get(d quad.Direction) graph.Value {
	switch d {
	case quad.Subject:
		return q.Subject
	case quad.Predicate:
		return q.Predicate
	case quad.Object:
		return q.Object
	case quad.Label:
		return q.Label
	default:
		return nil
	}
}

// Set assigns a specified direction of the quad to a given value.
func (q InternalQuad) Set(d quad.Direction, v graph.Value) {
	switch d {
	case quad.Subject:
		q.Subject = v
	case quad.Predicate:
		q.Predicate = v
	case quad.Object:
		q.Object = v
	case quad.Label:
		q.Label = v
	default:
		panic(d)
	}
}

// QuadIndexer is an optional interface for quad stores that keep an index of quad directions.
//
// It is used to optimize shapes based on stats from these indexes.
type QuadIndexer interface {
	// SizeOfIndex returns a size of a quad index with given constraints.
	SizeOfIndex(c map[quad.Direction]graph.Value) (int64, bool)
	// LookupQuadIndex finds a quad that matches a given constraint.
	// It returns false if quad was not found, or there are multiple quads matching constraint.
	LookupQuadIndex(c map[quad.Direction]graph.Value) (InternalQuad, bool)
}

// IsNull safely checks if shape represents an empty set. It accounts for both Null and nil.
func IsNull(s Shape) bool {
	_, ok := s.(Null)
	return s == nil || ok
}

// BuildIterator optimizes the shape and builds a corresponding iterator tree.
func BuildIterator(qs graph.QuadStore, s Shape) graph.Iterator {
	if s != nil {
		if clog.V(2) {
			clog.Infof("shape: %#v", s)
		}
		s, _ = Optimize(s, qs)
		if clog.V(2) {
			clog.Infof("optimized: %#v", s)
		}
	}
	if IsNull(s) {
		return iterator.NewNull()
	}
	return s.BuildIterator(qs)
}

// Null represent an empty set. Mostly used as a safe alias for nil shape.
type Null struct{}

func (Null) BuildIterator(qs graph.QuadStore) graph.Iterator {
	return iterator.NewNull()
}
func (s Null) Optimize(r Optimizer) (Shape, bool) {
	if r != nil {
		return r.OptimizeShape(s)
	}
	return nil, true
}

// AllNodes represents all nodes in QuadStore.
type AllNodes struct{}

func (s AllNodes) BuildIterator(qs graph.QuadStore) graph.Iterator {
	return qs.NodesAllIterator()
}
func (s AllNodes) Optimize(r Optimizer) (Shape, bool) {
	if r != nil {
		return r.OptimizeShape(s)
	}
	return s, false
}

// Except excludes a set on nodes from a source. If source is nil, AllNodes is assumed.
type Except struct {
	Exclude Shape // nodes to exclude
	From    Shape // a set of all nodes to exclude from; nil means AllNodes
}

func (s Except) BuildIterator(qs graph.QuadStore) graph.Iterator {
	var all graph.Iterator
	if s.From != nil {
		all = s.From.BuildIterator(qs)
	} else {
		all = qs.NodesAllIterator()
	}
	if IsNull(s.Exclude) {
		return all
	}
	return iterator.NewNot(s.Exclude.BuildIterator(qs), all)
}
func (s Except) Optimize(r Optimizer) (Shape, bool) {
	var opt bool
	s.Exclude, opt = s.Exclude.Optimize(r)
	if s.From != nil {
		var opta bool
		s.From, opta = s.From.Optimize(r)
		opt = opt || opta
	}
	if r != nil {
		ns, nopt := r.OptimizeShape(s)
		return ns, opt || nopt
	}
	if IsNull(s.Exclude) {
		return AllNodes{}, true
	} else if _, ok := s.Exclude.(AllNodes); ok {
		return nil, true
	}
	return s, opt
}

// ValueFilter is an interface for iterator wrappers that can filter node values.
type ValueFilter interface {
	BuildIterator(qs graph.QuadStore, it graph.Iterator) graph.Iterator
}

// Filter filters all values from the source using a list of operations.
type Filter struct {
	From    Shape         // source that will be filtered
	Filters []ValueFilter // filters to apply
}

func (s Filter) BuildIterator(qs graph.QuadStore) graph.Iterator {
	if IsNull(s.From) {
		return iterator.NewNull()
	}
	it := s.From.BuildIterator(qs)
	for _, f := range s.Filters {
		it = f.BuildIterator(qs, it)
	}
	return it
}
func (s Filter) Optimize(r Optimizer) (Shape, bool) {
	if IsNull(s.From) {
		return nil, true
	}
	var opt bool
	s.From, opt = s.From.Optimize(r)
	if r != nil {
		ns, nopt := r.OptimizeShape(s)
		return ns, opt || nopt
	}
	if IsNull(s.From) {
		return nil, true
	} else if len(s.Filters) == 0 {
		return s.From, true
	}
	return s, opt
}

var _ ValueFilter = Comparison{}

// Comparison is a value filter that evaluates binary operation in reference to a fixed value.
type Comparison struct {
	Op  iterator.Operator
	Val quad.Value
}

func (f Comparison) BuildIterator(qs graph.QuadStore, it graph.Iterator) graph.Iterator {
	return iterator.NewComparison(it, f.Op, f.Val, qs)
}

var _ ValueFilter = Regexp{}

// Regexp filters values using regular expression.
type Regexp struct {
	Re   *regexp.Regexp
	Refs bool // allow to match IRIs
}

func (f Regexp) BuildIterator(qs graph.QuadStore, it graph.Iterator) graph.Iterator {
	rit := iterator.NewRegex(it, f.Re, qs)
	rit.AllowRefs(f.Refs)
	return rit
}

// Count returns a count of objects in source as a single value. It always returns exactly one value.
type Count struct {
	Values Shape
}

func (s Count) BuildIterator(qs graph.QuadStore) graph.Iterator {
	var it graph.Iterator
	if IsNull(s.Values) {
		it = iterator.NewNull()
	} else {
		it = s.Values.BuildIterator(qs)
	}
	return iterator.NewCount(it, qs)
}
func (s Count) Optimize(r Optimizer) (Shape, bool) {
	if IsNull(s.Values) {
		return Fixed{graph.PreFetched(quad.Int(0))}, true
	}
	var opt bool
	s.Values, opt = s.Values.Optimize(r)
	if IsNull(s.Values) {
		return Fixed{graph.PreFetched(quad.Int(0))}, true
	}
	if r != nil {
		ns, nopt := r.OptimizeShape(s)
		return ns, opt || nopt
	}
	// TODO: ask QS to estimate size - if it exact, then we can use it
	return s, opt
}

// QuadFilter is a constraint used to filter quads that have a certain set of values on a given direction.
// Analog of LinksTo iterator.
type QuadFilter struct {
	Dir    quad.Direction
	Values Shape
}

// buildIterator is not exposed to force to use Quads and group filters together.
func (s QuadFilter) buildIterator(qs graph.QuadStore) graph.Iterator {
	if s.Values == nil {
		return iterator.NewNull()
	} else if v, ok := One(s.Values); ok {
		return qs.QuadIterator(s.Dir, v)
	}
	if s.Dir == quad.Any {
		panic("direction is not set")
	}
	sub := s.Values.BuildIterator(qs)
	return iterator.NewLinksTo(qs, sub, s.Dir)
}

// Quads is a selector of quads with a given set of node constraints. Empty or nil Quads is equivalent to AllQuads.
// Equivalent to And(AllQuads,LinksTo*) iterator tree.
type Quads []QuadFilter

func (s *Quads) Intersect(q ...QuadFilter) {
	*s = append(*s, q...)
}
func (s Quads) BuildIterator(qs graph.QuadStore) graph.Iterator {
	if len(s) == 0 {
		return qs.QuadsAllIterator()
	}
	its := make([]graph.Iterator, 0, len(s))
	for _, f := range s {
		its = append(its, f.buildIterator(qs))
	}
	if len(its) == 1 {
		return its[0]
	}
	return iterator.NewAnd(qs, its...)
}
func (s Quads) Optimize(r Optimizer) (Shape, bool) {
	var opt bool
	sw := 0
	realloc := func() {
		if !opt {
			opt = true
			nq := make(Quads, len(s))
			copy(nq, s)
			s = nq
		}
	}
	// TODO: multiple constraints on the same dir -> merge as Intersect on Values of this dir
	for i := 0; i < len(s); i++ {
		f := s[i]
		if f.Values == nil {
			return nil, true
		}
		v, ok := f.Values.Optimize(r)
		if v == nil {
			return nil, true
		}
		if ok {
			realloc()
			s[i].Values = v
		}
		switch s[i].Values.(type) {
		case Fixed:
			realloc()
			s[sw], s[i] = s[i], s[sw]
			sw++
		}
	}
	if r != nil {
		ns, nopt := r.OptimizeShape(s)
		return ns, opt || nopt
	}
	return s, opt
}

// NodesFrom extracts nodes on a given direction from source quads. Similar to HasA iterator.
type NodesFrom struct {
	Dir   quad.Direction
	Quads Shape
}

func (s NodesFrom) BuildIterator(qs graph.QuadStore) graph.Iterator {
	if IsNull(s.Quads) {
		return iterator.NewNull()
	}
	sub := s.Quads.BuildIterator(qs)
	if s.Dir == quad.Any {
		panic("direction is not set")
	}
	return iterator.NewHasA(qs, sub, s.Dir)
}
func (s NodesFrom) Optimize(r Optimizer) (Shape, bool) {
	if IsNull(s.Quads) {
		return nil, true
	}
	var opt bool
	s.Quads, opt = s.Quads.Optimize(r)
	if r != nil {
		// ignore default optimizations
		ns, nopt := r.OptimizeShape(s)
		return ns, opt || nopt
	}
	q, ok := s.Quads.(Quads)
	if !ok {
		return s, opt
	}
	// HasA(x, LinksTo(x, y)) == y
	if len(q) == 1 && q[0].Dir == s.Dir {
		return q[0].Values, true
	}
	// collect all fixed tags and push them up the tree
	var (
		tags  map[string]graph.Value
		nquad Quads
	)
	for i, f := range q {
		if ft, ok := f.Values.(FixedTags); ok {
			if tags == nil {
				// allocate map and clone quad filters
				tags = make(map[string]graph.Value)
				nquad = make([]QuadFilter, len(q))
				copy(nquad, q)
				q = nquad
			}
			q[i].Values = ft.On
			for k, v := range ft.Tags {
				tags[k] = v
			}
		}
	}
	if tags != nil {
		// re-run optimization without fixed tags
		ns, _ := NodesFrom{Dir: s.Dir, Quads: q}.Optimize(r)
		return FixedTags{On: ns, Tags: tags}, true
	}
	var (
		// if quad filter contains one fixed value, it will be added to the map
		filt map[quad.Direction]graph.Value
		// if we see a Save from AllNodes, we will write it here, since it's a Save on quad direction
		save map[quad.Direction][]string
		// how many filters are recognized
		n int
	)
	for _, f := range q {
		if v, ok := One(f.Values); ok {
			if filt == nil {
				filt = make(map[quad.Direction]graph.Value)
			}
			if _, ok := filt[f.Dir]; ok {
				return s, opt // just to be safe
			}
			filt[f.Dir] = v
			n++
		} else if sv, ok := f.Values.(Save); ok {
			if _, ok = sv.From.(AllNodes); ok {
				if save == nil {
					save = make(map[quad.Direction][]string)
				}
				save[f.Dir] = append(save[f.Dir], sv.Tags...)
				n++
			}
		}
	}
	if n == len(q) {
		// if all filters were recognized we can merge this tree as a single iterator with multiple
		// constraints and multiple save commands over the same set of quads
		ns, _ := QuadsAction{
			Result: s.Dir, // this is still a HasA, remember?
			Filter: filt,
			Save:   save,
		}.Optimize(r)
		return ns, true
	}
	// TODO
	return s, opt
}

// QuadsAction represents a set of actions that can be done to a set of quads in a single scan pass.
// It filters quads according to Filter constraints (equivalent of LinksTo), tags directions using tags in Save field
// and returns a specified quad direction as result of the iterator (equivalent of HasA).
// Optionally, Size field may be set to indicate an approximate number of quads that will be returned by this query.
type QuadsAction struct {
	Size   int64 // approximate size; zero means undefined
	Result quad.Direction
	Save   map[quad.Direction][]string
	Filter map[quad.Direction]graph.Value
}

func (s QuadsAction) Clone() QuadsAction {
	if n := len(s.Save); n != 0 {
		s2 := make(map[quad.Direction][]string, n)
		for k, v := range s.Save {
			s2[k] = v
		}
		s.Save = s2
	} else {
		s.Save = nil
	}
	if n := len(s.Filter); n != 0 {
		f2 := make(map[quad.Direction]graph.Value, n)
		for k, v := range s.Filter {
			f2[k] = v
		}
		s.Filter = f2
	} else {
		s.Filter = nil
	}
	return s
}
func (s QuadsAction) BuildIterator(qs graph.QuadStore) graph.Iterator {
	q := make(Quads, 0, len(s.Save)+len(s.Filter))
	for dir, val := range s.Filter {
		q = append(q, QuadFilter{Dir: dir, Values: Fixed{val}})
	}
	for dir, tags := range s.Save {
		q = append(q, QuadFilter{Dir: dir, Values: Save{From: AllNodes{}, Tags: tags}})
	}
	h := NodesFrom{Dir: s.Result, Quads: q}
	return h.BuildIterator(qs)
}
func (s QuadsAction) Optimize(r Optimizer) (Shape, bool) {
	if r != nil {
		return r.OptimizeShape(s)
	}
	// if optimizer has stats for quad indexes we can use them to do more
	ind, ok := r.(QuadIndexer)
	if !ok {
		return s, false
	}
	if s.Size > 0 { // already optimized; specific for QuadIndexer optimization
		return s, false
	}
	sz, exact := ind.SizeOfIndex(s.Filter)
	if !exact {
		return s, false
	}
	s.Size = sz // computing size is already an optimization
	if sz == 0 {
		// nothing here, collapse the tree
		return nil, true
	} else if sz == 1 {
		// only one quad matches this set of filters
		// try to load it from quad store, do all operations and bake result as a fixed node/tags
		if q, ok := ind.LookupQuadIndex(s.Filter); ok {
			fx := Fixed{q.Get(s.Result)}
			if len(s.Save) == 0 {
				return fx, true
			}
			ft := FixedTags{On: fx, Tags: make(map[string]graph.Value)}
			for d, tags := range s.Save {
				for _, t := range tags {
					ft.Tags[t] = q.Get(d)
				}
			}
			return ft, true
		}
	}
	if sz < int64(MaterializeThreshold) {
		// if this set is small enough - materialize it
		return Materialize{Values: s, Size: int(sz)}, true
	}
	return s, true
}

// One checks if Shape represents a single fixed value and returns it.
func One(s Shape) (graph.Value, bool) {
	switch s := s.(type) {
	case Fixed:
		if len(s) == 1 {
			return s[0], true
		}
	}
	return nil, false
}

// Fixed is a static set of nodes. Defined only for a particular QuadStore.
type Fixed []graph.Value

func (s *Fixed) Add(v ...graph.Value) {
	*s = append(*s, v...)
}
func (s Fixed) BuildIterator(qs graph.QuadStore) graph.Iterator {
	it := qs.FixedIterator()
	for _, v := range s {
		if _, ok := v.(quad.Value); ok {
			panic("quad value in fixed iterator")
		}
		it.Add(v)
	}
	return it
}
func (s Fixed) Optimize(r Optimizer) (Shape, bool) {
	if len(s) == 0 {
		return nil, true
	}
	if r != nil {
		return r.OptimizeShape(s)
	}
	return s, false
}

// FixedTags adds a set of fixed tag values to query results. It does not affect query execution in any other way.
//
// Shape implementations should try to push these objects up the tree during optimization process.
type FixedTags struct {
	Tags map[string]graph.Value
	On   Shape
}

func (s FixedTags) BuildIterator(qs graph.QuadStore) graph.Iterator {
	if IsNull(s.On) {
		return iterator.NewNull()
	}
	it := s.On.BuildIterator(qs)
	tg := it.Tagger()
	for k, v := range s.Tags {
		tg.AddFixed(k, v)
	}
	return it
}
func (s FixedTags) Optimize(r Optimizer) (Shape, bool) {
	if IsNull(s.On) {
		return nil, true
	}
	var opt bool
	s.On, opt = s.On.Optimize(r)
	if len(s.Tags) == 0 {
		return s.On, true
	} else if s2, ok := s.On.(FixedTags); ok {
		tags := make(map[string]graph.Value, len(s.Tags)+len(s2.Tags))
		for k, v := range s.Tags {
			tags[k] = v
		}
		for k, v := range s2.Tags {
			tags[k] = v
		}
		s, opt = FixedTags{On: s2.On, Tags: tags}, true
	}
	if r != nil {
		ns, nopt := r.OptimizeShape(s)
		return ns, opt || nopt
	}
	return s, opt
}

// Lookup is a static set of values that must be resolved to nodes by QuadStore.
type Lookup []quad.Value

func (s *Lookup) Add(v ...quad.Value) {
	*s = append(*s, v...)
}

var _ valueResolver = graph.QuadStore(nil)

type valueResolver interface {
	ValueOf(v quad.Value) graph.Value
}

func (s Lookup) resolve(qs valueResolver) Shape {
	// TODO: check if QS supports batch lookup
	vals := make([]graph.Value, 0, len(s))
	for _, v := range s {
		if gv := qs.ValueOf(v); gv != nil {
			vals = append(vals, gv)
		}
	}
	if len(vals) == 0 {
		return nil
	}
	return Fixed(vals)
}
func (s Lookup) BuildIterator(qs graph.QuadStore) graph.Iterator {
	f := s.resolve(qs)
	if IsNull(f) {
		return iterator.NewNull()
	}
	return f.BuildIterator(qs)
}
func (s Lookup) Optimize(r Optimizer) (Shape, bool) {
	if r == nil {
		return s, false
	}
	ns, opt := r.OptimizeShape(s)
	if opt {
		return ns, true
	}
	if qs, ok := r.(valueResolver); ok {
		ns, opt = s.resolve(qs), true
	}
	return ns, opt
}

var MaterializeThreshold = 100 // TODO: tune

// Materialize loads results of sub-query into memory during execution to speedup iteration.
type Materialize struct {
	Size   int // approximate size; zero means undefined
	Values Shape
}

func (s Materialize) BuildIterator(qs graph.QuadStore) graph.Iterator {
	if IsNull(s.Values) {
		return iterator.NewNull()
	}
	it := s.Values.BuildIterator(qs)
	return iterator.NewMaterializeWithSize(it, int64(s.Size))
}
func (s Materialize) Optimize(r Optimizer) (Shape, bool) {
	if IsNull(s.Values) {
		return nil, true
	}
	var opt bool
	s.Values, opt = s.Values.Optimize(r)
	if r != nil {
		ns, nopt := r.OptimizeShape(s)
		return ns, opt || nopt
	}
	return s, opt
}

func clearFixedTags(arr []Shape) ([]Shape, map[string]graph.Value) {
	var tags map[string]graph.Value
	for i := 0; i < len(arr); i++ {
		if ft, ok := arr[i].(FixedTags); ok {
			if tags == nil {
				tags = make(map[string]graph.Value)
				na := make([]Shape, len(arr))
				copy(na, arr)
				arr = na
			}
			arr[i] = ft.On
			for k, v := range ft.Tags {
				tags[k] = v
			}
		}
	}
	return arr, tags
}

// Intersect computes an intersection of nodes between multiple queries. Similar to And iterator.
type Intersect []Shape

func (s Intersect) BuildIterator(qs graph.QuadStore) graph.Iterator {
	if len(s) == 0 {
		return iterator.NewNull()
	}
	sub := make([]graph.Iterator, 0, len(s))
	for _, c := range s {
		sub = append(sub, c.BuildIterator(qs))
	}
	if len(sub) == 1 {
		return sub[0]
	}
	return iterator.NewAnd(qs, sub...)
}
func (s Intersect) Optimize(r Optimizer) (sout Shape, opt bool) {
	if len(s) == 0 {
		return nil, true
	}
	// function to lazily reallocate a copy of Intersect slice
	realloc := func() {
		if !opt {
			arr := make(Intersect, len(s))
			copy(arr, s)
			s = arr
		}
	}
	// optimize sub-iterators, return empty set if Null is found
	for i := 0; i < len(s); i++ {
		c := s[i]
		if IsNull(c) {
			return nil, true
		}
		v, ok := c.Optimize(r)
		if !ok {
			continue
		}
		realloc()
		opt = true
		if IsNull(v) {
			return nil, true
		}
		s[i] = v
	}
	if r != nil {
		ns, nopt := r.OptimizeShape(s)
		return ns, opt || nopt
	}
	if arr, ft := clearFixedTags([]Shape(s)); ft != nil {
		ns, _ := FixedTags{On: Intersect(arr), Tags: ft}.Optimize(r)
		return ns, true
	}
	var (
		onlyAll = true   // contains only AllNodes shapes
		fixed   []Fixed  // we will collect all Fixed, and will place it as a first iterator
		tags    []string // if we find a Save inside, we will push it outside of Intersect
		quads   Quads    // also, collect all quad filters into a single set
	)
	remove := func(i *int, optimized bool) {
		realloc()
		if optimized {
			opt = true
		}
		v := *i
		s = append(s[:v], s[v+1:]...)
		v--
		*i = v
	}
	// second pass - remove AllNodes, merge Quads, collect Fixed, collect Save, merge Intersects
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c := c.(type) {
		case AllNodes: // remove AllNodes - it's useless
			remove(&i, true)
			// prevent resetting of onlyAll
			continue
		case Optional:
			if IsNull(c.From) {
				remove(&i, true)
				// prevent resetting of onlyAll
				continue
			}
		case Quads: // merge all quad filters
			remove(&i, false)
			if quads == nil {
				quads = c[:len(c):len(c)]
			} else {
				opt = true
				quads = append(quads, c...)
			}
		case Fixed: // collect all Fixed sets
			remove(&i, true)
			fixed = append(fixed, c)
		case Intersect: // merge with other Intersects
			remove(&i, true)
			s = append(s, c...)
		case Save: // push Save outside of Intersect
			realloc()
			opt = true
			tags = append(tags, c.Tags...)
			s[i] = c.From
			i--
		}
		onlyAll = false
	}
	if onlyAll {
		return AllNodes{}, true
	}
	if len(tags) != 0 {
		// don't forget to move Save outside of Intersect at the end
		defer func() {
			if IsNull(sout) {
				return
			}
			sv := Save{From: sout, Tags: tags}
			var topt bool
			sout, topt = sv.Optimize(r)
			opt = opt || topt
		}()
	}
	if quads != nil {
		nq, qopt := quads.Optimize(r)
		if IsNull(nq) {
			return nil, true
		}
		opt = opt || qopt
		s = append(s, nq)
	}
	// TODO: intersect fixed
	if len(fixed) == 1 {
		fix := fixed[0]
		if len(s) == 1 {
			// try to push fixed down the tree
			switch sf := s[0].(type) {
			case QuadsAction:
				// TODO: accept an array of Fixed values
				if len(fix) == 1 {
					// we have a single value in Fixed that is intersected with HasA tree
					// this means we can add a new constraint: LinksTo(HasA.Dir, fixed)
					// result direction of HasA will be preserved
					fv := fix[0]
					if v := sf.Filter[sf.Result]; v != nil {
						// we have the same direction set as a fixed constraint - do filtering
						if graph.ToKey(v) != graph.ToKey(fv) {
							return nil, true
						} else {
							return sf, true
						}
					}
					sf = sf.Clone()
					sf.Filter[sf.Result] = fv // LinksTo(HasA.Dir, fixed)
					sf.Size = 0               // re-calculate size
					ns, _ := sf.Optimize(r)
					return ns, true
				}
			case NodesFrom:
				if sq, ok := sf.Quads.(Quads); ok {
					// an optimization above is valid for NodesFrom+Quads as well
					// we can add the same constraint to Quads and remove Fixed
					qi := -1
					for i, qf := range sq {
						if qf.Dir == sf.Dir {
							qi = i
							break
						}
					}
					if qi < 0 {
						// no filter on this direction - append
						sf.Quads = append(Quads{
							{Dir: sf.Dir, Values: fix},
						}, sq...)
					} else {
						// already have a filter on this direction - push Fixed inside it
						sq = append(Quads{}, sq...)
						sf.Quads = sq
						qf := &sq[qi]
						qf.Values = IntersectShapes(fix, qf.Values)
					}
					return sf, true
				}
			}
		}
		// place fixed as a first iterator
		s = append(s, nil)
		copy(s[1:], s)
		s[0] = fix
	} else if len(fixed) > 1 {
		ns := make(Intersect, len(s)+len(fixed))
		for i, f := range fixed {
			ns[i] = f
		}
		copy(ns[len(fixed):], s)
		s = ns
	}
	if len(s) == 0 {
		return nil, true
	} else if len(s) == 1 {
		return s[0], true
	}
	// TODO: optimize order
	return s, opt
}

// Union joins results of multiple queries together. It does not make results unique.
type Union []Shape

func (s Union) BuildIterator(qs graph.QuadStore) graph.Iterator {
	if len(s) == 0 {
		return iterator.NewNull()
	}
	sub := make([]graph.Iterator, 0, len(s))
	for _, c := range s {
		sub = append(sub, c.BuildIterator(qs))
	}
	if len(sub) == 1 {
		return sub[0]
	}
	return iterator.NewOr(sub...)
}
func (s Union) Optimize(r Optimizer) (Shape, bool) {
	var opt bool
	realloc := func() {
		if !opt {
			arr := make(Union, len(s))
			copy(arr, s)
			s = arr
		}
	}
	// optimize subiterators
	for i := 0; i < len(s); i++ {
		c := s[i]
		v, ok := c.Optimize(r)
		if !ok {
			continue
		}
		realloc()
		opt = true
		s[i] = v
	}
	if r != nil {
		ns, nopt := r.OptimizeShape(s)
		return ns, opt || nopt
	}
	if arr, ft := clearFixedTags([]Shape(s)); ft != nil {
		ns, _ := FixedTags{On: Union(arr), Tags: ft}.Optimize(r)
		return ns, true
	}
	// second pass - remove Null
	for i := 0; i < len(s); i++ {
		c := s[i]
		if IsNull(c) {
			realloc()
			opt = true
			s = append(s[:i], s[i+1:]...)
		}
	}
	if len(s) == 0 {
		return nil, true
	} else if len(s) == 1 {
		return s[0], true
	}
	// TODO: join Fixed
	return s, opt
}

// Page provides a simple form of pagination. Can be used to skip or limit results.
type Page struct {
	From  Shape
	Skip  int64
	Limit int64 // zero means unlimited
}

func (s Page) BuildIterator(qs graph.QuadStore) graph.Iterator {
	if IsNull(s.From) {
		return iterator.NewNull()
	}
	it := s.From.BuildIterator(qs)
	if s.Skip > 0 {
		it = iterator.NewSkip(it, s.Skip)
	}
	if s.Limit > 0 {
		it = iterator.NewLimit(it, s.Limit)
	}
	return it
}
func (s Page) Optimize(r Optimizer) (Shape, bool) {
	if IsNull(s.From) {
		return nil, true
	}
	var opt bool
	s.From, opt = s.From.Optimize(r)
	if s.Skip <= 0 && s.Limit <= 0 {
		return s.From, true
	}
	if p, ok := s.From.(Page); ok {
		p2 := p.ApplyPage(s)
		if p2 == nil {
			return nil, true
		}
		s, opt = *p2, true
	}
	if r != nil {
		ns, nopt := r.OptimizeShape(s)
		return ns, opt || nopt
	}
	// TODO: check size
	return s, opt
}
func (s Page) ApplyPage(p Page) *Page {
	s.Skip += p.Skip
	if s.Limit > 0 {
		s.Limit -= p.Skip
		if s.Limit <= 0 {
			return nil
		}
		if p.Limit > 0 && s.Limit > p.Limit {
			s.Limit = p.Limit
		}
	} else {
		s.Limit = p.Limit
	}
	return &s
}

// Unique makes query results unique.
type Unique struct {
	From Shape
}

func (s Unique) BuildIterator(qs graph.QuadStore) graph.Iterator {
	if IsNull(s.From) {
		return iterator.NewNull()
	}
	it := s.From.BuildIterator(qs)
	return iterator.NewUnique(it)
}
func (s Unique) Optimize(r Optimizer) (Shape, bool) {
	if IsNull(s.From) {
		return nil, true
	}
	var opt bool
	s.From, opt = s.From.Optimize(r)
	if IsNull(s.From) {
		return nil, true
	}
	if r != nil {
		ns, nopt := r.OptimizeShape(s)
		return ns, opt || nopt
	}
	return s, opt
}

// Save tags a results of query with provided tags.
type Save struct {
	Tags []string
	From Shape
}

func (s Save) BuildIterator(qs graph.QuadStore) graph.Iterator {
	if IsNull(s.From) {
		return iterator.NewNull()
	}
	it := s.From.BuildIterator(qs)
	tg := it.Tagger()
	if len(s.Tags) != 0 {
		tg.Add(s.Tags...)
	}
	return it
}
func (s Save) Optimize(r Optimizer) (Shape, bool) {
	if IsNull(s.From) {
		return nil, true
	}
	var opt bool
	s.From, opt = s.From.Optimize(r)
	if len(s.Tags) == 0 {
		return s.From, true
	}
	if r != nil {
		ns, nopt := r.OptimizeShape(s)
		return ns, opt || nopt
	}
	return s, opt
}

// Optional makes a query execution optional. The query can only produce tagged results,
// since it's value is not used to compute intersection.
type Optional struct {
	From Shape
}

func (s Optional) BuildIterator(qs graph.QuadStore) graph.Iterator {
	if IsNull(s.From) {
		return iterator.NewOptional(iterator.NewNull())
	}
	return iterator.NewOptional(s.From.BuildIterator(qs))
}
func (s Optional) Optimize(r Optimizer) (Shape, bool) {
	if IsNull(s.From) {
		return s, false
	}
	var opt bool
	s.From, opt = s.From.Optimize(r)
	if IsNull(s.From) {
		return s, opt
	}
	if r != nil {
		ns, nopt := r.OptimizeShape(s)
		return ns, opt || nopt
	}
	return s, opt
}
