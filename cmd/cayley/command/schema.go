package command

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/cayleygraph/cayley/internal/linkedql/schema"
)

func NewSchemaCommand() *cobra.Command {
	root := &cobra.Command{
		Use:   "schema",
		Short: "Commands related to RDF schema",
	}
	root.AddCommand(
		NewLinkedQLSchemaCommand(),
	)
	return root
}

func NewLinkedQLSchemaCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "linkedql",
		Short: "Generate LinkedQL Schema to stdout",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				return fmt.Errorf("too many arguments provided, expected 0")
			}
			data := schema.Generate()
			buf := bytes.NewBuffer(nil)
			err := json.Indent(buf, data, "", "\t")
			if err != nil {
				return err
			}
			fmt.Println(buf)
			return nil
		},
	}
}
