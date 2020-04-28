package main

import (
	"io"
	"net/http"
	"os"
	"path/filepath"

	// Load all supported quad formats.

	"github.com/cayleygraph/quad"
	_ "github.com/cayleygraph/quad/jsonld"
	_ "github.com/cayleygraph/quad/nquads"

	"github.com/spf13/cobra"
)

const defaultFormat = "jsonld"

func main() {
	var uri, formatName string

	var cmd = &cobra.Command{
		Use:   "cayleyexport <file>",
		Short: "Export data from Cayley. If no file is provided, cayleyexport writes to stdout.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var format *quad.Format
			var file *os.File
			if formatName != "" {
				format = quad.FormatByName(formatName)
			}
			if len(args) == 0 {
				file = os.Stdout
			}
			if len(args) == 1 {
				fileName := args[0]
				if formatName == "" {
					format = quad.FormatByExt(filepath.Ext(fileName))
				}
				var err error
				file, err = os.Create(fileName)
				if err != nil {
					return err
				}
				defer file.Close()
			}
			if format == nil {
				format = quad.FormatByName(defaultFormat)
			}
			req, err := http.NewRequest("GET", uri+"/api/v2/read", nil)
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
			_, err = io.Copy(file, resp.Body)
			if err != nil {
				return err
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&uri, "uri", "", "http://127.0.0.1:64210", "Cayley URI connection string")
	cmd.Flags().StringVarP(&formatName, "format", "", "", "format of the provided data (if can not be detected defaults to JSON-LD)")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
