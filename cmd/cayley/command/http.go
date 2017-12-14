package command

import (
	"net"
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/internal"
	chttp "github.com/cayleygraph/cayley/internal/http"
	"github.com/cayleygraph/cayley/quad"
)

func NewHttpCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "http",
		Short: "Serve an HTTP endpoint on the given host and port.",
		RunE: func(cmd *cobra.Command, args []string) error {
			printBackendInfo()
			p := mustSetupProfile(cmd)
			defer mustFinishProfile(p)

			timeout := viper.GetDuration(keyQueryTimeout)
			if init, err := cmd.Flags().GetBool("init"); err != nil {
				return err
			} else if init {
				if err = initDatabase(); err == graph.ErrDatabaseExists {
					clog.Infof("database already initialized, skipping init")
				} else if err != nil {
					return err
				}
			}
			h, err := openDatabase()
			if err != nil {
				return err
			}
			defer h.Close()

			ro := viper.GetBool(KeyReadOnly)
			if load, _ := cmd.Flags().GetString(flagLoad); load != "" {
				typ, _ := cmd.Flags().GetString(flagLoadFormat)
				// TODO: check read-only flag in config before that?
				start := time.Now()
				if err = internal.Load(h.QuadWriter, quad.DefaultBatch, load, typ); err != nil {
					return err
				}
				clog.Infof("loaded %q in %v", load, time.Since(start))
			}

			err = chttp.SetupRoutes(h, &chttp.Config{
				Timeout:  timeout,
				ReadOnly: ro,
			})
			if err != nil {
				return err
			}
			host, _ := cmd.Flags().GetString("host")
			phost := host
			if host, port, err := net.SplitHostPort(host); err == nil && host == "" {
				phost = net.JoinHostPort("localhost", port)
			}
			clog.Infof("listening on %s, web interface at http://%s", host, phost)
			return http.ListenAndServe(host, nil)
		},
	}
	cmd.Flags().String("host", "127.0.0.1:64210", "host:port to listen on")
	cmd.Flags().Bool("init", false, "initialize the database before using it")
	cmd.Flags().DurationP("timeout", "t", 30*time.Second, "elapsed time until an individual query times out")
	cmd.Flags().StringVar(&chttp.AssetsPath, "assets", "", "explicit path to the HTTP assets")
	registerLoadFlags(cmd)
	viper.BindPFlag(keyQueryTimeout, cmd.Flags().Lookup("timeout"))
	return cmd
}
