// Package voc implements an RDF namespace (vocabulary) registry.
package voc

import (
	"strings"
	"sync"
)

// Namespace is a RDF namespace (vocabulary).
type Namespace struct {
	Full   string
	Prefix string
}

type ByFullName []Namespace

func (o ByFullName) Len() int           { return len(o) }
func (o ByFullName) Less(i, j int) bool { return o[i].Full < o[j].Full }
func (o ByFullName) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

// Namespaces is a set of registered namespaces.
type Namespaces struct {
	Safe     bool // if set, assume no locking is required
	mu       sync.RWMutex
	prefixes map[string]string
}

// Register adds namespace to registered list.
func (p *Namespaces) Register(ns Namespace) {
	if !p.Safe {
		p.mu.Lock()
		defer p.mu.Unlock()
	}
	if p.prefixes == nil {
		p.prefixes = make(map[string]string)
	}
	p.prefixes[ns.Prefix] = ns.Full
}

// ShortIRI replaces a base IRI of a known vocabulary with it's prefix.
//
//	ShortIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") // returns "rdf:type"
func (p *Namespaces) ShortIRI(iri string) string {
	if !p.Safe {
		p.mu.RLock()
		defer p.mu.RUnlock()
	}
	for pref, ns := range p.prefixes {
		if strings.HasPrefix(iri, ns) {
			return pref + iri[len(ns):]
		}
	}
	return iri
}

// FullIRI replaces known prefix in IRI with it's full vocabulary IRI.
//
//	FullIRI("rdf:type") // returns "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
func (p *Namespaces) FullIRI(iri string) string {
	if !p.Safe {
		p.mu.RLock()
		defer p.mu.RUnlock()
	}
	for pref, ns := range p.prefixes {
		if strings.HasPrefix(iri, pref) {
			return ns + iri[len(pref):]
		}
	}
	return iri
}

// List enumerates all registered namespace pairs.
func (p *Namespaces) List() (out []Namespace) {
	if !p.Safe {
		p.mu.RLock()
		defer p.mu.RUnlock()
	}
	out = make([]Namespace, 0, len(p.prefixes))
	for pref, ns := range p.prefixes {
		out = append(out, Namespace{Prefix: pref, Full: ns})
	}
	return
}

// Clone makes a copy of namespaces list.
func (p *Namespaces) Clone() *Namespaces {
	if !p.Safe {
		p.mu.RLock()
		defer p.mu.RUnlock()
	}
	p2 := Namespaces{
		prefixes: make(map[string]string, len(p.prefixes)),
	}
	for pref, ns := range p.prefixes {
		p2.prefixes[pref] = ns
	}
	return &p2
}

// CloneTo adds registered namespaces to a given list.
func (p *Namespaces) CloneTo(p2 *Namespaces) {
	if p == p2 {
		return
	}
	if !p.Safe {
		p.mu.RLock()
		defer p.mu.RUnlock()
	}
	if !p2.Safe {
		p2.mu.Lock()
		defer p2.mu.Unlock()
	}
	if p2.prefixes == nil {
		p2.prefixes = make(map[string]string, len(p.prefixes))
	}
	for pref, ns := range p.prefixes {
		p2.prefixes[pref] = ns
	}
}

var global Namespaces

// Register adds namespace to a global registered list.
func Register(ns Namespace) {
	global.Register(ns)
}

// RegisterPrefix globally associates a given prefix with a base vocabulary IRI.
func RegisterPrefix(pref string, ns string) {
	Register(Namespace{Prefix: pref, Full: ns})
}

// ShortIRI replaces a base IRI of a known vocabulary with it's prefix.
//
//	ShortIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") // returns "rdf:type"
func ShortIRI(iri string) string {
	return global.ShortIRI(iri)
}

// FullIRI replaces known prefix in IRI with it's full vocabulary IRI.
//
//	FullIRI("rdf:type") // returns "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
func FullIRI(iri string) string {
	return global.FullIRI(iri)
}

// List enumerates all registered namespace pairs.
func List() []Namespace {
	return global.List()
}

// Clone makes a copy of global namespaces list.
func Clone() *Namespaces {
	return global.Clone()
}

// CloneTo adds all global namespaces to a given list.
func CloneTo(p *Namespaces) {
	global.CloneTo(p)
}
