package command

import (
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/internal"
	"github.com/cayleygraph/quad"
)

func newLazyReader(open func() (quad.ReadCloser, error)) quad.ReadCloser {
	return &lazyReader{open: open}
}

type lazyReader struct {
	rc   quad.ReadCloser
	open func() (quad.ReadCloser, error)
}

func (r *lazyReader) ReadQuad() (quad.Quad, error) {
	if r.rc == nil {
		rc, err := r.open()
		if err != nil {
			return quad.Quad{}, err
		}
		r.rc = rc
	}
	return r.rc.ReadQuad()
}
func (r *lazyReader) Close() (err error) {
	if r.rc != nil {
		err = r.rc.Close()
	}
	return
}

type multiReader struct {
	rc []quad.ReadCloser
	i  int
}

func (r *multiReader) ReadQuad() (quad.Quad, error) {
	for {
		if r.i >= len(r.rc) {
			return quad.Quad{}, io.EOF
		}
		rc := r.rc[r.i]
		q, err := rc.ReadQuad()
		if err == io.EOF {
			rc.Close()
			r.i++
			continue
		}
		return q, err
	}
}
func (r *multiReader) Close() error {
	var first error
	if r.i < len(r.rc) {
		for _, rc := range r.rc[r.i:] {
			if err := rc.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return nil
}

func NewConvertCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "convert",
		Aliases: []string{"conv"},
		Short:   "Convert quad files between supported formats.",
		RunE: func(cmd *cobra.Command, args []string) error {
			dump, _ := cmd.Flags().GetString(flagDump)
			dumpf, _ := cmd.Flags().GetString(flagDumpFormat)
			if dump == "" && len(args) > 0 {
				i := len(args) - 1
				dump, args = args[i], args[:i]
			}

			var files []string
			if load, _ := cmd.Flags().GetString(flagLoad); load != "" {
				files = append(files, load)
			}
			files = append(files, args...)
			if len(files) == 0 || dump == "" {
				return errors.New("both input and output files must be specified")
			}
			loadf, _ := cmd.Flags().GetString(flagLoadFormat)
			var multi multiReader
			for _, path := range files {
				path := path
				multi.rc = append(multi.rc, newLazyReader(func() (quad.ReadCloser, error) {
					if dump == "-" {
						clog.Infof("reading %q", path)
					} else {
						fmt.Printf("reading %q\n", path)
					}
					return internal.QuadReaderFor(path, loadf)
				}))
			}
			// TODO: print additional stats
			return writerQuadsTo(dump, dumpf, &multi)
		},
	}
	registerLoadFlags(cmd)
	registerDumpFlags(cmd)
	return cmd
}
