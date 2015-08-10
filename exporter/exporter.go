package exporter

import (
	"io"
	"encoding/json"

	"github.com/google/cayley/graph"
)

type Exporter struct {
	wr io.Writer
	qstore graph.QuadStore
	err error
	count int32
}

func NewExporter(writer io.Writer, qstore graph.QuadStore) *Exporter {
	return &Exporter{wr: writer, qstore: qstore}
}

// number of records
func (exp *Exporter) Count() int32 {
	return exp.count
}

func (exp *Exporter) ExportJson() {
	var jstr []byte 
	exp.Write("[")
	it := exp.qstore.QuadsAllIterator()
	for graph.Next(it) {
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
