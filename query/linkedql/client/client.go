package client

import (
	"github.com/cayleygraph/quad"
)

type Path map[string]interface{}

// As assigns the resolved values of the from step to a given name. The name can be used with the Select and Documents steps to retreive the values or to return to the values in further steps with the Back step. It resolves to the values of the from step.
func (p Path) As(name string) Path {
	return Path{"@type": "http://cayley.io/linkedql#As", "from": p, "http://cayley.io/linkedql#name": name}
}

// Back resolves to the values of the previous the step or the values assigned to name in a former step.
func (p Path) Back(name string) Path {
	return Path{"@type": "http://cayley.io/linkedql#Back", "from": p, "http://cayley.io/linkedql#name": name}
}

// Count resolves to the number of the resolved values of the from step
func (p Path) Count() Path {
	return Path{"@type": "http://cayley.io/linkedql#Count", "from": p}
}

// Difference resolves to all the values resolved by the from step different then the values resolved by the provided steps. Caution: it might be slow to execute.
func (p Path) Difference(steps []Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#Difference", "from": p, "http://cayley.io/linkedql#steps": steps}
}

// Filter applies constraints to a set of nodes. Can be used to filter values by range or match strings.
func (p Path) Filter() Path {
	return Path{"@type": "http://cayley.io/linkedql#Filter", "from": p}
}

// Follow is the way to use a path prepared with Morphism. Applies the path chain on the morphism object to the current path. Starts as if at the g.M() and follows through the morphism path.
func (p Path) Follow(followed Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#Follow", "from": p, "http://cayley.io/linkedql#followed": followed}
}

// FollowReverse is the same as follow but follows the chain in the reverse direction. Flips View and ViewReverse where appropriate, the net result being a virtual predicate followed in the reverse direction. Starts at the end of the morphism and follows it backwards (with appropriate flipped directions) to the g.M() location.
func (p Path) FollowReverse(followed Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#FollowReverse", "from": p, "http://cayley.io/linkedql#followed": followed}
}

// Has filters all paths which are, at this point, on the subject for the given predicate and object, but do not follow the path, merely filter the possible paths. Usually useful for starting with all nodes, or limiting to a subset depending on some predicate/value pair.
func (p Path) Has(property Path, values []quad.Value) Path {
	return Path{"@type": "http://cayley.io/linkedql#Has", "from": p, "http://cayley.io/linkedql#property": property, "http://cayley.io/linkedql#values": values}
}

// HasReverse is the same as Has, but sets constraint in reverse direction.
func (p Path) HasReverse(property Path, values []quad.Value) Path {
	return Path{"@type": "http://cayley.io/linkedql#HasReverse", "from": p, "http://cayley.io/linkedql#property": property, "http://cayley.io/linkedql#values": values}
}

// In aliases for ViewReverse
func (p Path) In(properties Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#In", "from": p, "http://cayley.io/linkedql#properties": properties}
}

// Intersect resolves to all the same values resolved by the from step and the provided steps.
func (p Path) Intersect(steps []Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#Intersect", "from": p, "http://cayley.io/linkedql#steps": steps}
}

// Is resolves to all the values resolved by the from step which are included in provided values.
func (p Path) Is(values []quad.Value) Path {
	return Path{"@type": "http://cayley.io/linkedql#Is", "from": p, "http://cayley.io/linkedql#values": values}
}

// Labels gets the list of inbound and outbound quad labels
func (p Path) Labels() Path {
	return Path{"@type": "http://cayley.io/linkedql#Labels", "from": p}
}

// Limit limits a number of nodes for current path.
func (p Path) Limit() Path {
	return Path{"@type": "http://cayley.io/linkedql#Limit", "from": p}
}

// Order sorts the results in ascending order according to the current entity / value
func (p Path) Order() Path {
	return Path{"@type": "http://cayley.io/linkedql#Order", "from": p}
}

// Out aliases for View
func (p Path) Out(properties Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#Out", "from": p, "http://cayley.io/linkedql#properties": properties}
}

// Placeholder is like Vertex but resolves to the values in the context it is placed in. It should only be used where a PathStep is expected and can't be resolved on its own.
func Placeholder() Path {
	return Path{"@type": "http://cayley.io/linkedql#Placeholder"}
}

// Properties adds tags for all properties of the current entity
func (p Path) Properties(names []string) Path {
	return Path{"@type": "http://cayley.io/linkedql#Properties", "from": p, "http://cayley.io/linkedql#names": names}
}

// PropertyNames gets the list of predicates that are pointing out from a node.
func (p Path) PropertyNames() Path {
	return Path{"@type": "http://cayley.io/linkedql#PropertyNames", "from": p}
}

// PropertyNamesAs tags the list of predicates that are pointing out from a node.
func (p Path) PropertyNamesAs(tag string) Path {
	return Path{"@type": "http://cayley.io/linkedql#PropertyNamesAs", "from": p, "http://cayley.io/linkedql#tag": tag}
}

// ReverseProperties gets all the properties the current entity / value is referenced at
func (p Path) ReverseProperties(names []string) Path {
	return Path{"@type": "http://cayley.io/linkedql#ReverseProperties", "from": p, "http://cayley.io/linkedql#names": names}
}

// ReversePropertyNames gets the list of predicates that are pointing in to a node.
func (p Path) ReversePropertyNames() Path {
	return Path{"@type": "http://cayley.io/linkedql#ReversePropertyNames", "from": p}
}

// ReversePropertyNamesAs tags the list of predicates that are pointing in to a node.
func (p Path) ReversePropertyNamesAs(tag string) Path {
	return Path{"@type": "http://cayley.io/linkedql#ReversePropertyNamesAs", "from": p, "http://cayley.io/linkedql#tag": tag}
}

// Select Select returns flat records of tags matched in the query
func (p Path) Select(tags []string) Path {
	return Path{"@type": "http://cayley.io/linkedql#Select", "from": p, "http://cayley.io/linkedql#tags": tags}
}

// SelectFirst Like Select but only returns the first result
func (p Path) SelectFirst(tags []string) Path {
	return Path{"@type": "http://cayley.io/linkedql#SelectFirst", "from": p, "http://cayley.io/linkedql#tags": tags}
}

// Skip skips a number of nodes for current path.
func (p Path) Skip() Path {
	return Path{"@type": "http://cayley.io/linkedql#Skip", "from": p}
}

// Union returns the combined paths of the two queries. Notice that it's per-path, not per-node. Once again, if multiple paths reach the same destination, they might have had different ways of getting there (and different tags).
func (p Path) Union(steps []Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#Union", "from": p, "http://cayley.io/linkedql#steps": steps}
}

// Unique removes duplicate values from the path.
func (p Path) Unique() Path {
	return Path{"@type": "http://cayley.io/linkedql#Unique", "from": p}
}

// Value Value returns a single value matched in the query
func (p Path) Value() Path {
	return Path{"@type": "http://cayley.io/linkedql#Value", "from": p}
}

// Vertex resolves to all the existing objects and primitive values in the graph. If provided with values resolves to a sublist of all the existing values in the graph.
func Vertex(values []quad.Value) Path {
	return Path{"@type": "http://cayley.io/linkedql#Vertex", "http://cayley.io/linkedql#values": values}
}

// View resolves to the values of the given property or properties in via of the current objects. If via is a path it's resolved values will be used as properties.
func (p Path) View(properties Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#View", "from": p, "http://cayley.io/linkedql#properties": properties}
}

// ViewBoth is like View but resolves to both the object values and references to the values of the given properties in via. It is the equivalent for the Union of View and ViewReverse of the same property.
func (p Path) ViewBoth(properties Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#ViewBoth", "from": p, "http://cayley.io/linkedql#properties": properties}
}

// ViewReverse is the inverse of View. Starting with the nodes in `path` on the object, follow the quads with predicates defined by `predicatePath` to their subjects.
func (p Path) ViewReverse(properties Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#ViewReverse", "from": p, "http://cayley.io/linkedql#properties": properties}
}
