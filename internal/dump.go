package internal

import (
	"fmt"
	"os"

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

	export := exporter.NewExporter(f, qs)
	if export.Err() != nil {
		return export.Err()
	}

        if outFile != "-" {
		fmt.Printf("%d entries were written\n", export.Count())
	}
	return nil
}
