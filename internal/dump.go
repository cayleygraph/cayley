package internal

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
)

// Dump the content of the database into a file based
// on a few different formats
func Dump(qs graph.QuadStore, outFile, typ string) error {
	var f *os.File
	if outFile == "-" {
		f = os.Stdout
	} else {
		var err error
		f, err = os.Create(outFile)
		if err != nil {
			return fmt.Errorf("could not open file %q: %v", outFile, err)
		}
		defer f.Close()
		fmt.Printf("dumping db to file %q\n", outFile)
	}

	var w io.Writer = f
	if filepath.Ext(outFile) == ".gz" {
		gzip := gzip.NewWriter(f)
		defer gzip.Close()
		w = gzip
	}
	if typ == "quad" {
		typ = "nquads"
	}
	format := quad.FormatByName(typ)
	if format == nil {
		return fmt.Errorf("unsupported format: %q", typ)
	} else if format.Writer == nil {
		return fmt.Errorf("encoding in %s format is not supported", typ)
	}
	qw := format.Writer(w)
	defer qw.Close()

	//TODO: add possible support for exporting specific queries only
	qr := graph.NewQuadStoreReader(qs)
	defer qr.Close()

	n, err := quad.Copy(qw, qr)
	if err != nil {
		return err
	} else if err = qw.Close(); err != nil {
		return err
	}
	if outFile != "-" {
		fmt.Printf("%d entries were written\n", n)
	}
	return nil
}
