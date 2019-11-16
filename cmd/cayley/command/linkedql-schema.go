package command

import (
	"encoding/json"
	"fmt"
	"github.com/cayleygraph/cayley/query/linkedql/schema"
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
			s := schema.Generate()
			bytes, err := json.MarshalIndent(s, "", "    ")
			if err != nil {
				panic(err)
			}
			fmt.Println(string(bytes))

			return nil
		},
	}
}
