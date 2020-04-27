package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
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
		Use:   "cayleyimport <file>",
		Short: "Import data into Cayley. If no file is provided, cayleyimport reads from stdin.",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var format *quad.Format
			var file *os.File
			if formatName != "" {
				format = quad.FormatByName(formatName)
			}
			if len(args) == 0 {
				file = os.Stdin
			}
			if len(args) == 1 {
				fileName := args[0]
				if formatName == "" {
					format = quad.FormatByExt(filepath.Ext(fileName))
				}
				var err error
				file, err = os.Open(fileName)
				if err != nil {
					log.Fatal(err)
					os.Exit(1)
				}
			}
			if format == nil {
				format = quad.FormatByName("jsonld")
			}
			r, err := http.Post(uri+"/api/v2/write", format.Mime[0], file)
			if err != nil {
				log.Fatal(err)
				os.Exit(1)
			}
			defer r.Body.Close()
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Fatal(err)
				os.Exit(1)
			}
			var response struct {
				Result string `json:"result"`
				Count  string `json:"count"`
			}
			json.Unmarshal(body, &response)
			fmt.Println(response.Result)
		},
	}

	cmd.Flags().StringVarP(&uri, "uri", "", "http://127.0.0.1:64210", "Cayley URI connection string")
	cmd.Flags().StringVarP(&formatName, "format", "", "", "format of the provided data (if can not be detected defaults to JSON-LD)")

	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
