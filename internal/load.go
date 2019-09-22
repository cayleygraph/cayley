package internal

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/internal/decompressor"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/nquads"
)

// Load loads a graph from the given path and write it to qw.  See
// DecompressAndLoad for more information.
func Load(qw quad.WriteCloser, batch int, path, typ string) error {
	return DecompressAndLoad(qw, batch, path, typ)
}

type readCloser struct {
	quad.ReadCloser
	close func() error
}

func (r readCloser) Close() error {
	err := r.ReadCloser.Close()
	if r.close != nil {
		r.close()
	}
	return err
}

type nopCloser struct {
	quad.Reader
}

func (r nopCloser) Close() error { return nil }

func QuadReaderFor(path, typ string) (quad.ReadCloser, error) {
	var (
		r io.Reader
		c io.Closer
	)
	if path == "-" {
		r = os.Stdin
	} else if u, err := url.Parse(path); err != nil || u.Scheme == "file" || u.Scheme == "" {
		// Don't alter relative URL path or non-URL path parameter.
		if u.Scheme != "" && err == nil {
			// Recovery heuristic for mistyping "file://path/to/file".
			path = filepath.Join(u.Host, u.Path)
		}
		f, err := os.Open(path)
		if os.IsNotExist(err) {
			return nil, err
		} else if err != nil {
			return nil, fmt.Errorf("could not open file %q: %v", path, err)
		}
		r, c = f, f
	} else {
		res, err := http.Get(path)
		if err != nil {
			return nil, fmt.Errorf("could not get resource <%s>: %v", u, err)
		}
		// TODO(dennwc): save content type for format auto-detection
		r, c = res.Body, res.Body
	}

	r, err := decompressor.New(r)
	if err != nil {
		if c != nil {
			c.Close()
		}
		if err == io.EOF {
			return nopCloser{quad.NewReader(nil)}, nil
		}
		return nil, err
	}

	var qr quad.ReadCloser
	switch typ {
	case "cquad", "nquad": // legacy
		qr = nquads.NewReader(r, false)
	default:
		var format *quad.Format
		if typ == "" {
			name := filepath.Base(path)
			name = strings.TrimSuffix(name, ".gz")
			name = strings.TrimSuffix(name, ".bz2")
			format = quad.FormatByExt(filepath.Ext(name))
			if format == nil {
				typ = "nquads"
			}
		}
		if format == nil {
			format = quad.FormatByName(typ)
		}
		if format == nil {
			err = fmt.Errorf("unknown quad format %q", typ)
		} else if format.Reader == nil {
			err = fmt.Errorf("decoding of %q is not supported", typ)
		}
		if err != nil {
			if c != nil {
				c.Close()
			}
			return nil, err
		}
		qr = format.Reader(r)
	}
	if c != nil {
		return readCloser{ReadCloser: qr, close: c.Close}, nil
	}
	return qr, nil
}

// DecompressAndLoad will load or fetch a graph from the given path, decompress
// it, and then call the given load function to process the decompressed graph.
// If no loadFn is provided, db.Load is called.
func DecompressAndLoad(qw quad.WriteCloser, batch int, path, typ string) error {
	if path == "" {
		return nil
	}
	qr, err := QuadReaderFor(path, typ)
	if err != nil {
		return err
	}
	defer qr.Close()

	_, err = quad.CopyBatch(&batchLogger{w: qw}, qr, batch)
	if err != nil {
		return fmt.Errorf("db: failed to load data: %v", err)
	}
	return qw.Close()
}

type batchLogger struct {
	cnt int
	w   quad.Writer
}

func (w *batchLogger) WriteQuads(quads []quad.Quad) (int, error) {
	n, err := w.w.WriteQuads(quads)
	if clog.V(2) {
		w.cnt += n
		clog.Infof("Wrote %d quads.", w.cnt)
	}
	return n, err
}
