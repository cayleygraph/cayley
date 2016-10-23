package command

import (
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/net/context"

	"github.com/cayleygraph/cayley/internal"
	"github.com/cayleygraph/cayley/internal/db"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/query"
)

func NewReplCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "repl",
		Short: "Drop into a REPL of the given query language.",
		RunE: func(cmd *cobra.Command, args []string) error {
			printBackendInfo()
			timeout, err := cmd.Flags().GetDuration("timeout")
			if err != nil {
				return err
			}
			if init, err := cmd.Flags().GetBool("init"); err != nil {
				return err
			} else if init {
				if err = initDatabase(); err != nil {
					return err
				}
			}
			h, err := openDatabase()
			if err != nil {
				return err
			}
			defer h.Close()

			if load, _ := cmd.Flags().GetString(flagLoad); load != "" {
				typ, _ := cmd.Flags().GetString(flagLoadFormat)
				// TODO: check read-only flag in config before that?
				if err = internal.Load(h.QuadWriter, quad.DefaultBatch, load, typ); err != nil {
					return err
				}
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ch := make(chan os.Signal, 1)
			signal.Notify(ch, os.Interrupt)
			go func() {
				select {
				case <-ch:
				case <-ctx.Done():
				}
				signal.Stop(ch)
				cancel()
			}()
			lang, _ := cmd.Flags().GetString("lang")
			return db.Repl(ctx, h, lang, timeout)
		},
	}
	langs := query.Languages()
	cmd.Flags().Bool("init", false, "initialize the database before using it")
	cmd.Flags().StringP("lang", "l", "gremlin", `query language to use ("`+strings.Join(langs, `", "`)+`")`)
	cmd.Flags().DurationP("timeout", "t", 30*time.Second, "elapsed time until an individual query times out")
	registerLoadFlags(cmd)
	return cmd
}
