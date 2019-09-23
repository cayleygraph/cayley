package command

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/internal/repl"
	"github.com/cayleygraph/cayley/query"
)

const (
	keyQueryTimeout = "query.timeout"
)

func getContext() (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
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
	return ctx, cancel
}

func registerQueryFlags(cmd *cobra.Command) {
	langs := query.Languages()
	cmd.Flags().Bool("init", false, "initialize the database before using it")
	cmd.Flags().String("lang", "gizmo", `query language to use ("`+strings.Join(langs, `", "`)+`")`)
	cmd.Flags().DurationP("timeout", "t", 30*time.Second, "elapsed time until an individual query times out")
	viper.BindPFlag(keyQueryTimeout, cmd.Flags().Lookup("timeout"))
	registerLoadFlags(cmd)
}

func NewReplCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "repl",
		Short: "Drop into a REPL of the given query language.",
		RunE: func(cmd *cobra.Command, args []string) error {
			printBackendInfo()
			p := mustSetupProfile(cmd)
			defer mustFinishProfile(p)

			h, err := openForQueries(cmd)
			if err != nil {
				return err
			}
			defer h.Close()

			ctx, cancel := getContext()
			defer cancel()

			timeout := viper.GetDuration("timeout")
			lang, _ := cmd.Flags().GetString("lang")
			return repl.Repl(ctx, h, lang, timeout)
		},
	}
	registerQueryFlags(cmd)
	return cmd
}

func NewQueryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "query",
		Aliases: []string{"qu"},
		Short:   "Run a query in a specified database and print results.",
		RunE: func(cmd *cobra.Command, args []string) error {
			var querystr string
			if len(args) == 0 {
				bytes, err := ioutil.ReadAll(os.Stdin)
				if err != nil {
					return fmt.Errorf("Error occured while reading from stdin : %s.", err)
				}
				querystr = string(bytes)
			} else if len(args) == 1 {
				querystr = args[0]
			} else {
				return fmt.Errorf("Query accepts only one argument, the query string or nothing for reading from stdin.")
			}
			clog.Infof("Query:\n%s", querystr)
			printBackendInfo()
			p := mustSetupProfile(cmd)
			defer mustFinishProfile(p)

			h, err := openForQueries(cmd)
			if err != nil {
				return err
			}
			defer h.Close()

			ctx, cancel := getContext()
			defer cancel()

			timeout := viper.GetDuration("timeout")
			if timeout > 0 {
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			lang, _ := cmd.Flags().GetString("lang")
			limit, err := cmd.Flags().GetInt("limit")
			if err != nil {
				return err
			}
			enc := json.NewEncoder(os.Stdout)
			it, err := query.Execute(ctx, h, lang, querystr, query.Options{
				Collation: query.JSON,
				Limit:     limit,
			})
			if err != nil {
				return err
			}
			defer it.Close()
			for i := 0; it.Next(ctx) && (limit <= 0 || i < limit); i++ {
				if err = enc.Encode(it.Result()); err != nil {
					return err
				}
			}
			return it.Err()
		},
	}
	registerQueryFlags(cmd)
	cmd.Flags().IntP("limit", "n", 100, "limit a number of results")
	return cmd
}
