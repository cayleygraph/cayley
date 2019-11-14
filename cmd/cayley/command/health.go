package command

import (
	"fmt"
	"log"
	"net/http"

	"github.com/spf13/cobra"
)

const defaultAddress = "http://localhost:64210/"

func NewHealthCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "health",
		Aliases: []string{},
		Short:   "Health check HTTP server",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 {
				return fmt.Errorf("Too many arguments provided, expected 0 or 1")
			}
			address := defaultAddress
			if len(args) == 1 {
				address = args[0]
			}
			healthAddress := address + "health"
			resp, err := http.Get(healthAddress)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 204 {
				return fmt.Errorf("/health responded with status code %d, expected 204", resp.StatusCode)
			}
			log.Printf("%s ok", healthAddress)
			return nil
		},
	}
}
