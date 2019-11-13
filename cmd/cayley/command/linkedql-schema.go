package command

import (
	"encoding/json"
	"fmt"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/spf13/cobra"
)

func NewLinkedQLSchemaCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "linkedql-schema",
		Aliases: []string{},
		Short:   "Generate LinkedQL Schema to stdout",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				return fmt.Errorf("Too many arguments provided, expected 0")
			}
			schema := linkedql.GenerateSchema()
			bytes, err := json.MarshalIndent(schema, "", "    ")
			if err != nil {
				panic(err)
			}
			fmt.Println(string(bytes))

			return nil
		},
	}
}
