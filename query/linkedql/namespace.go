package linkedql

import "github.com/cayleygraph/quad/voc"

const (
	Namespace = "http://cayley.io/linkedql#"
	Prefix    = "linkedql:"
)

func init() {
	voc.Register(voc.Namespace{Full: Namespace, Prefix: Prefix})
}
