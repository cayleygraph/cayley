// Package owl contains constants of the Web Ontology Language (OWL)
package owl

import "github.com/cayleygraph/quad/voc"

func init() {
	voc.RegisterPrefix(Prefix, NS)
}

const (
	NS     = `http://www.w3.org/2002/07/owl#`
	Prefix = `owl:`
)

const (
	UnionOf        = NS + "unionOf"
	Restriction    = NS + "Restriction"
	OnProperty     = NS + "onProperty"
	Cardinality    = NS + "cardinality"
	MaxCardinality = NS + "maxCardinality"
)
