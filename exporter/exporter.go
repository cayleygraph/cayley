package exporter

import (
	"encoding/json"
	"io"
	"strconv"

	"github.com/google/cayley/graph"
)

type Exporter struct {
	wr     io.Writer
	qstore graph.QuadStore
	qi     graph.Iterator
	err    error
	count  int
}

func NewExporter(writer io.Writer, qstore graph.QuadStore) *Exporter {
	return NewExporterForIterator(writer, qstore, qstore.QuadsAllIterator())
}

func NewExporterForIterator(writer io.Writer, qstore graph.QuadStore, qi graph.Iterator) *Exporter {
	return &Exporter{wr: writer, qstore: qstore, qi: qi}
}

// number of records
func (exp *Exporter) Count() int {
	return exp.count
}

func (exp *Exporter) ExportQuad() {
	exp.qi.Reset()
	for it := exp.qi; graph.Next(it); {
		exp.count++
		quad := exp.qstore.Quad(it.Result())

		exp.WriteEscString(quad.Subject)
		exp.Write(" ")
		exp.WriteEscString(quad.Predicate)
		exp.Write(" ")
		exp.WriteEscString(quad.Object)
		if quad.Label != "" {
			exp.Write(" ")
			exp.WriteEscString(quad.Label)
		}
		exp.Write(" .\n")
	}
}

func (exp *Exporter) ExportJson() {
	var jstr []byte
	exp.Write("[")
	exp.qi.Reset()
	for it := exp.qi; graph.Next(it); {
		exp.count++
		if exp.count > 1 {
			exp.Write(",")
		}

		jstr, exp.err = json.Marshal(exp.qstore.Quad(it.Result()))
		if exp.err != nil {
			return
		}
		exp.Write(string(jstr[:]))
	}
	exp.Write("]\n")
}

//experimental
func (exp *Exporter) ExportGml() {
	var seen map[string]int32 // todo eliminate this for large dbs
	var id int32

	exp.Write("Creator Cayley\ngraph\n[\n")

	seen = make(map[string]int32)
	exp.qi.Reset()
	for it := exp.qi; graph.Next(it); {
		cur := exp.qstore.Quad(it.Result())
		if _, ok := seen[cur.Subject]; !ok {
			exp.Write("  node\n  [\n    id ")
			seen[cur.Subject] = id
			exp.Write(strconv.FormatInt(int64(id), 10))
			exp.Write("\n    label ")
			exp.WriteEscString(cur.Subject)
			exp.Write("\n  ]\n")
			id++
		}
		if _, ok := seen[cur.Object]; !ok {
			exp.Write("  node\n  [\n    id ")
			seen[cur.Object] = id
			exp.Write(strconv.FormatInt(int64(id), 10))
			exp.Write("\n    label ")
			exp.WriteEscString(cur.Object)
			exp.Write("\n  ]\n")
			id++
		}
		exp.count++
	}

	exp.qi.Reset()
	for it := exp.qi; graph.Next(it); {
		cur := exp.qstore.Quad(it.Result())
		exp.Write("  edge\n  [\n    source ")
		exp.Write(strconv.FormatInt(int64(seen[cur.Subject]), 10))
		exp.Write("\n    target ")
		exp.Write(strconv.FormatInt(int64(seen[cur.Object]), 10))
		exp.Write("\n    label ")
		exp.WriteEscString(cur.Predicate)
		exp.Write("\n  ]\n")
		exp.count++
	}
	exp.Write("]\n")
}

//experimental
func (exp *Exporter) ExportGraphml() {
	var seen map[string]bool // eliminate this for large databases

	exp.Write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	exp.Write("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n")
	exp.Write("   xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n")
	exp.Write("   xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n")
	exp.Write("  <graph id=\"Caylay\" edgedefault=\"directed\">\n")

	seen = make(map[string]bool)
	exp.qi.Reset()
	for it := exp.qi; graph.Next(it); {
		cur := exp.qstore.Quad(it.Result())
		if found := seen[cur.Subject]; !found {
			seen[cur.Subject] = true
			exp.Write("    <node id=")
			exp.WriteEscString(cur.Subject)
			exp.Write(" />\n")
		}
		if found := seen[cur.Object]; !found {
			seen[cur.Object] = true
			exp.Write("    <node id=")
			exp.WriteEscString(cur.Object)
			exp.Write(" />\n")
		}
		exp.count++
	}

	exp.qi.Reset()
	for it := exp.qi; graph.Next(it); {
		cur := exp.qstore.Quad(it.Result())
		exp.Write("    <edge source=")
		exp.WriteEscString(cur.Subject)
		exp.Write(" target=")
		exp.WriteEscString(cur.Object)
		exp.Write(">\n")
		exp.Write("      <data key=\"predicate\">")
		exp.Write(cur.Predicate)
		exp.Write("</data>\n    </edge>\n")
		exp.count++
	}
	exp.Write("  </graph>\n</graphml>\n")
}

//print out the string quoted, escaped
func (exp *Exporter) WriteEscString(str string) {
	var esc []byte

	if exp.err != nil {
		return
	}
	esc, exp.err = json.Marshal(str)
	if exp.err != nil {
		return
	}
	_, exp.err = exp.wr.Write(esc)
}

func (exp *Exporter) Write(str string) {
	if exp.err != nil {
		return
	}
	_, exp.err = exp.wr.Write([]byte(str))
}

func (exp *Exporter) Err() error {
	return exp.err
}
