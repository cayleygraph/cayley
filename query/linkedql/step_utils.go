package linkedql

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

// BuildFromPath creates a path of a given from
// In case from is nil defaults to placeholder
func BuildFromPath(qs graph.QuadStore, ns *voc.Namespaces, from PathStep) (*path.Path, error) {
	if from == nil {
		return path.StartMorphism(), nil
	}
	fromPath, err := from.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath, nil
}
