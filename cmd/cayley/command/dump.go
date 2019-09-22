package command

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
)

func writerQuadsTo(path string, typ string, qr quad.Reader) error {
	var f *os.File
	if path == "-" {
		f = os.Stdout
		clog.Infof("writing quads to stdout")
	} else {
		var err error
		f, err = os.Create(path)
		if err != nil {
			return fmt.Errorf("could not create file %q: %v", path, err)
		}
		defer f.Close()
		fmt.Printf("writing quads to file %q\n", path)
	}

	var w io.Writer = f
	ext := filepath.Ext(path)
	if ext == ".gz" {
		ext = filepath.Ext(strings.TrimSuffix(path, ext))
		gzip := gzip.NewWriter(f)
		defer gzip.Close()
		w = gzip
	}
	var format *quad.Format
	if typ == "" {
		format = quad.FormatByExt(ext)
		if format == nil {
			typ = "nquads"
		}
	}
	if format == nil {
		format = quad.FormatByName(typ)
	}
	if format == nil {
		return fmt.Errorf("unsupported format: %q", typ)
	} else if format.Writer == nil {
		return fmt.Errorf("encoding in %s format is not supported", typ)
	}
	qw := format.Writer(w)
	defer qw.Close()

	n, err := quad.Copy(qw, qr)
	if err != nil {
		return err
	} else if err = qw.Close(); err != nil {
		return err
	}
	if path != "-" {
		fmt.Printf("%d entries were written\n", n)
	}
	return nil
}

func dumpDatabase(h *graph.Handle, path string, typ string) error {
	//TODO: add possible support for exporting specific queries only
	qr := graph.NewQuadStoreReader(h.QuadStore)
	defer qr.Close()
	return writerQuadsTo(path, typ, qr)
}
