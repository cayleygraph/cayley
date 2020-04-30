package main

import (
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/cayleygraph/cayley/clog"

	// Load all supported quad formats.
	"github.com/cayleygraph/quad"
	_ "github.com/cayleygraph/quad/jsonld"
	_ "github.com/cayleygraph/quad/nquads"

	"github.com/spf13/cobra"
)

const defaultFormat = "jsonld"

// NewCmd creates the command
func NewCmd() *cobra.Command {
	var quiet bool
	var uri, formatName, out string

	var cmd = &cobra.Command{
		Use:   "cayleyexport",
		Short: "Export data from Cayley. If no file is provided, cayleyexport writes to stdout.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if quiet {
				clog.SetV(500)
			}
			var format *quad.Format
			var w io.Writer
			if formatName != "" {
				format = quad.FormatByName(formatName)
			}
			if out == "" {
				w = cmd.OutOrStdout()
			} else {
				if formatName == "" {
					format = formatByFileName(out)
					if format == nil {
						clog.Warningf("File has unknown extension %v. Defaulting to %v", out, defaultFormat)
					}
				}
				file, err := os.Create(out)
				if err != nil {
					return err
				}
				w = file
				defer file.Close()
			}
			if format == nil {
				format = quad.FormatByName(defaultFormat)
			}
			req, err := http.NewRequest(http.MethodGet, uri+"/api/v2/read", nil)
			req.Header.Set("Accept", format.Mime[0])
			if err != nil {
				return err
			}
			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			_, err = io.Copy(w, resp.Body)
			if err != nil {
				return err
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&uri, "uri", "", "http://127.0.0.1:64210", "Cayley URI connection string")
	cmd.Flags().StringVarP(&formatName, "format", "", "", "format of the provided data (if can not be detected defaults to JSON-LD)")
	cmd.Flags().StringVarP(&out, "out", "o", "", "output file; if not specified, stdout is used")
	cmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "hide all log output")

	return cmd
}

func main() {
	cmd := NewCmd()
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func formatByFileName(fileName string) *quad.Format {
	ext := filepath.Ext(fileName)
	return quad.FormatByExt(ext)
}
