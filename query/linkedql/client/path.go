package client

import (
	"github.com/cayleygraph/quad"
)

type Path map[string]interface{}

func (p Path) As(name string) Path {
	return Path{"@type": "http://cayley.io/linkedql#As", "from": p, "http://cayley.io/linkedql#name": name}
}
func (p Path) Back(name string) Path {
	return Path{"@type": "http://cayley.io/linkedql#Back", "from": p, "http://cayley.io/linkedql#name": name}
}
func (p Path) Count() Path {
	return Path{"@type": "http://cayley.io/linkedql#Count", "from": p}
}
func (p Path) Difference(steps []Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#Difference", "from": p, "http://cayley.io/linkedql#steps": steps}
}
func (p Path) Filter() Path {
	return Path{"@type": "http://cayley.io/linkedql#Filter", "from": p}
}
func (p Path) Follow(followed Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#Follow", "from": p, "http://cayley.io/linkedql#followed": followed}
}
func (p Path) FollowReverse(followed Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#FollowReverse", "from": p, "http://cayley.io/linkedql#followed": followed}
}
func (p Path) Has(property Path, values []quad.Value) Path {
	return Path{"@type": "http://cayley.io/linkedql#Has", "from": p, "http://cayley.io/linkedql#property": property, "http://cayley.io/linkedql#values": values}
}
func (p Path) HasReverse(property Path, values []quad.Value) Path {
	return Path{"@type": "http://cayley.io/linkedql#HasReverse", "from": p, "http://cayley.io/linkedql#property": property, "http://cayley.io/linkedql#values": values}
}
func (p Path) In(properties Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#In", "from": p, "http://cayley.io/linkedql#properties": properties}
}
func (p Path) Intersect(steps []Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#Intersect", "from": p, "http://cayley.io/linkedql#steps": steps}
}
func (p Path) Is(values []quad.Value) Path {
	return Path{"@type": "http://cayley.io/linkedql#Is", "from": p, "http://cayley.io/linkedql#values": values}
}
func (p Path) Labels() Path {
	return Path{"@type": "http://cayley.io/linkedql#Labels", "from": p}
}
func (p Path) Limit() Path {
	return Path{"@type": "http://cayley.io/linkedql#Limit", "from": p}
}
func (p Path) Order() Path {
	return Path{"@type": "http://cayley.io/linkedql#Order", "from": p}
}
func (p Path) Out(properties Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#Out", "from": p, "http://cayley.io/linkedql#properties": properties}
}
func Placeholder() Path {
	return Path{"@type": "http://cayley.io/linkedql#Placeholder"}
}
func (p Path) Properties(names []string) Path {
	return Path{"@type": "http://cayley.io/linkedql#Properties", "from": p, "http://cayley.io/linkedql#names": names}
}
func (p Path) PropertyNames() Path {
	return Path{"@type": "http://cayley.io/linkedql#PropertyNames", "from": p}
}
func (p Path) PropertyNamesAs(tag string) Path {
	return Path{"@type": "http://cayley.io/linkedql#PropertyNamesAs", "from": p, "http://cayley.io/linkedql#tag": tag}
}
func (p Path) ReverseProperties(names []string) Path {
	return Path{"@type": "http://cayley.io/linkedql#ReverseProperties", "from": p, "http://cayley.io/linkedql#names": names}
}
func (p Path) ReversePropertyNames() Path {
	return Path{"@type": "http://cayley.io/linkedql#ReversePropertyNames", "from": p}
}
func (p Path) ReversePropertyNamesAs(tag string) Path {
	return Path{"@type": "http://cayley.io/linkedql#ReversePropertyNamesAs", "from": p, "http://cayley.io/linkedql#tag": tag}
}
func (p Path) Select(tags []string) Path {
	return Path{"@type": "http://cayley.io/linkedql#Select", "from": p, "http://cayley.io/linkedql#tags": tags}
}
func (p Path) SelectFirst(tags []string) Path {
	return Path{"@type": "http://cayley.io/linkedql#SelectFirst", "from": p, "http://cayley.io/linkedql#tags": tags}
}
func (p Path) Skip() Path {
	return Path{"@type": "http://cayley.io/linkedql#Skip", "from": p}
}
func (p Path) Union(steps []Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#Union", "from": p, "http://cayley.io/linkedql#steps": steps}
}
func (p Path) Unique() Path {
	return Path{"@type": "http://cayley.io/linkedql#Unique", "from": p}
}
func (p Path) Value() Path {
	return Path{"@type": "http://cayley.io/linkedql#Value", "from": p}
}
func Vertex(values []quad.Value) Path {
	return Path{"@type": "http://cayley.io/linkedql#Vertex", "http://cayley.io/linkedql#values": values}
}
func (p Path) View(properties Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#View", "from": p, "http://cayley.io/linkedql#properties": properties}
}
func (p Path) ViewBoth(properties Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#ViewBoth", "from": p, "http://cayley.io/linkedql#properties": properties}
}
func (p Path) ViewReverse(properties Path) Path {
	return Path{"@type": "http://cayley.io/linkedql#ViewReverse", "from": p, "http://cayley.io/linkedql#properties": properties}
}
