package internal

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/nquads"
)

// Load loads a graph from the given path and write it to qw.  See
// DecompressAndLoad for more information.
func Load(qw graph.QuadWriter, batch int, path, typ string) error {
	return DecompressAndLoad(qw, batch, path, typ, nil)
}

// DecompressAndLoad will load or fetch a graph from the given path, decompress
// it, and then call the given load function to process the decompressed graph.
// If no loadFn is provided, db.Load is called.
func DecompressAndLoad(qw graph.QuadWriter, batch int, path, typ string, writerFunc func(graph.QuadWriter) quad.BatchWriter) error {
	var r io.Reader

	if path == "" {
		return nil
	}
	u, err := url.Parse(path)
	if err != nil || u.Scheme == "file" || u.Scheme == "" {
		// Don't alter relative URL path or non-URL path parameter.
		if u.Scheme != "" && err == nil {
			// Recovery heuristic for mistyping "file://path/to/file".
			path = filepath.Join(u.Host, u.Path)
		}
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("could not open file %q: %v", path, err)
		}
		defer f.Close()
		r = f
	} else {
		res, err := http.Get(path)
		if err != nil {
			return fmt.Errorf("could not get resource <%s>: %v", u, err)
		}
		defer res.Body.Close()
		r = res.Body
	}

	r, err = Decompressor(r)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}

	var qr quad.Reader
	switch typ {
	case "cquad":
		qr = nquads.NewReader(r, false)
	case "nquad":
		qr = nquads.NewReader(r, true)
	default:
		rf := quad.FormatByName(typ)
		if rf == nil {
			return fmt.Errorf("unknown quad format %q", typ)
		} else if rf.Reader == nil {
			return fmt.Errorf("decoding of %q is not supported", typ)
		}
		qr = rf.Reader(r)
	}

	if writerFunc == nil {
		writerFunc = graph.NewWriter
	}
	dest := writerFunc(qw)

	_, err = quad.CopyBatch(&batchLogger{BatchWriter: dest}, qr, batch)
	if err != nil {
		return fmt.Errorf("db: failed to load data: %v", err)
	}
	return nil
}

type batchLogger struct {
	cnt int
	quad.BatchWriter
}

func (w *batchLogger) WriteQuads(quads []quad.Quad) (int, error) {
	n, err := w.BatchWriter.WriteQuads(quads)
	if clog.V(2) {
		w.cnt += n
		clog.Infof("Wrote %d quads.", w.cnt)
	}
	return n, err
}
