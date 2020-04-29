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
	var uri, formatName, out string

	var cmd = &cobra.Command{
		Use:   "cayleyexport",
		Short: "Export data from Cayley. If no file is provided, cayleyexport writes to stdout.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			var format *quad.Format
			var w io.Writer
			if formatName != "" {
				format = quad.FormatByName(formatName)
			}
			if out == "" {
				w = cmd.OutOrStdout()
			} else {
				if formatName == "" {
					ext := filepath.Ext(out)
					format = quad.FormatByExt(ext)
					if format == nil {
						clog.Warningf("Unknown extension %v. Defaulting to %v", ext, defaultFormat)
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

	return cmd
}

func main() {
	cmd := NewCmd()
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
