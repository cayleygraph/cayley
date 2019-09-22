// Package voc is deprecated. Use github.com/cayleygraph/quad/voc.
package voc

import (
	"github.com/cayleygraph/quad/voc"
)

// Namespace is a RDF namespace (vocabulary).
//
// Deprecated: use github.com/cayleygraph/quad/voc package instead.
type Namespace = voc.Namespace

type ByFullName = voc.ByFullName

// Namespaces is a set of registered namespaces.
//
// Deprecated: use github.com/cayleygraph/quad/voc package instead.
type Namespaces = voc.Namespaces

// Register adds namespace to a global registered list.
//
// Deprecated: use github.com/cayleygraph/quad/voc package instead.
func Register(ns Namespace) {
	voc.Register(ns)
}

// RegisterPrefix globally associates a given prefix with a base vocabulary IRI.
//
// Deprecated: use github.com/cayleygraph/quad/voc package instead.
func RegisterPrefix(pref string, ns string) {
	voc.RegisterPrefix(pref, ns)
}

// ShortIRI replaces a base IRI of a known vocabulary with it's prefix.
//
//	ShortIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") // returns "rdf:type"
//
// Deprecated: use github.com/cayleygraph/quad/voc package instead.
func ShortIRI(iri string) string {
	return voc.ShortIRI(iri)
}

// FullIRI replaces known prefix in IRI with it's full vocabulary IRI.
//
//	FullIRI("rdf:type") // returns "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
//
// Deprecated: use github.com/cayleygraph/quad/voc package instead.
func FullIRI(iri string) string {
	return voc.FullIRI(iri)
}

// List enumerates all registered namespace pairs.
//
// Deprecated: use github.com/cayleygraph/quad/voc package instead.
func List() []Namespace {
	return voc.List()
}

// Clone makes a copy of global namespaces list.
//
// Deprecated: use github.com/cayleygraph/quad/voc package instead.
func Clone() *Namespaces {
	return voc.Clone()
}

// CloneTo adds all global namespaces to a given list.
//
// Deprecated: use github.com/cayleygraph/quad/voc package instead.
func CloneTo(p *Namespaces) {
	voc.CloneTo(p)
}
