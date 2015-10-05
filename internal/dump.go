package internal

import (
	"fmt"
	"os"
	"compress/gzip"
	"path/filepath"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/exporter"
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

	var export *exporter.Exporter
	if filepath.Ext(outFile) == ".gz" {
		gzip := gzip.NewWriter(f)
		defer gzip.Close()
		export = exporter.NewExporter(gzip, qs)
	} else {
		export = exporter.NewExporter(f, qs)
	}

	//TODO: add possible support for exporting specific queries only
	switch typ {
	case "quad":
		export.ExportQuad()
	case "json":
		export.ExportJson()
	// gml/graphml experimental
	case "gml":
		export.ExportGml()
	case "graphml":
		export.ExportGraphml()
	default:
		return fmt.Errorf("unknown format %q", typ)
	}
	
	if export.Err() != nil {
		return export.Err()
	}

        if outFile != "-" {
		fmt.Printf("%d entries were written\n", export.Count())
	}
	return nil
}
