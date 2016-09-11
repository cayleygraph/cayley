// Package voc implements an RDF namespace (vocabulary) registry.
package voc

import (
	"strings"
	"sync"
)

var (
	mu       sync.RWMutex
	prefixes map[string]string
)

// RegisterPrefix associates a given prefix with a base vocabulary IRI.
func RegisterPrefix(pref string, ns string) {
	mu.Lock()
	if prefixes == nil {
		prefixes = make(map[string]string)
	}
	prefixes[pref] = ns
	mu.Unlock()
}

// ShortIRI replaces a base IRI of a known vocabulary with it's prefix.
//
//	ShortIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") // returns "rdf:type"
func ShortIRI(iri string) string {
	for pref, ns := range prefixes {
		if strings.HasPrefix(iri, ns) {
			return pref + iri[len(ns):]
		}
	}
	return iri
}

// FullIRI replaces known prefix in IRI with it's full vocabulary IRI.
//
//	FullIRI("rdf:type") // returns "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
func FullIRI(iri string) string {
	for pref, ns := range prefixes {
		if strings.HasPrefix(iri, pref) {
			return ns + iri[len(pref):]
		}
	}
	return iri
}

// List enumerates all registered prefix-IRI pairs.
func List() (out [][2]string) {
	mu.RLock()
	defer mu.RUnlock()
	out = make([][2]string, 0, len(prefixes))
	for pref, ns := range prefixes {
		out = append(out, [2]string{pref, ns})
	}
	return
}
