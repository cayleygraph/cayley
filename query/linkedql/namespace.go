package linkedql

import "github.com/cayleygraph/quad/voc"

const namespace = "http://cayley.io/linkedql#"
const prefix = "linkedql:"

func init() {
	voc.Register(voc.Namespace{Full: namespace, Prefix: prefix})
}
