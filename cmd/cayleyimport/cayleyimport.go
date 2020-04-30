package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
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
	var uri, formatName string

	var cmd = &cobra.Command{
		Use:   "cayleyimport <file>",
		Short: "Import data into Cayley. If no file is provided, cayleyimport reads from stdin.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if quiet {
				clog.SetV(500)
			}
			var format *quad.Format
			var reader io.Reader
			if formatName != "" {
				format = quad.FormatByName(formatName)
			}
			if len(args) == 0 {
				in := cmd.InOrStdin()
				if !hasIn(in) {
					return errors.New("Either provide file to read from or pipe data")
				}
				reader = in
			} else {
				fileName := args[0]
				if formatName == "" {
					format = formatByFileName(fileName)
					if format == nil {
						clog.Warningf("File has unknown extension %v. Defaulting to %v", fileName, defaultFormat)
					}
				}
				file, err := os.Open(fileName)
				if err != nil {
					return err
				}
				defer file.Close()
				reader = file
			}
			if format == nil {
				format = quad.FormatByName(defaultFormat)
			}
			r, err := http.Post(uri+"/api/v2/write", format.Mime[0], reader)
			if err != nil {
				return err
			}
			defer r.Body.Close()
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				return err
			}
			if r.StatusCode == http.StatusOK {
				var response struct {
					Result string `json:"result"`
					Count  string `json:"count"`
					Error  string `json:"error"`
				}
				json.Unmarshal(body, &response)
				if response.Error != "" {
					return errors.New(response.Error)
				}
				if !quiet {
					fmt.Println(response.Result)
				}
			} else if r.StatusCode == http.StatusNotFound {
				return errors.New("Database instance does not support write")
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&uri, "uri", "", "http://127.0.0.1:64210", "Cayley URI connection string")
	cmd.Flags().StringVarP(&formatName, "format", "", "", "format of the provided data (if can not be detected defaults to JSON-LD)")
	cmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "hide all log output")
	return cmd
}

func main() {
	cmd := NewCmd()
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func hasIn(in io.Reader) bool {
	if in == os.Stdin {
		stat, _ := os.Stdin.Stat()
		return (stat.Mode() & os.ModeCharDevice) == 0
	}
	return true
}

func formatByFileName(fileName string) *quad.Format {
	ext := filepath.Ext(fileName)
	return quad.FormatByExt(ext)
}
