package command

import (
	"net"
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/cayleygraph/cayley/clog"
	chttp "github.com/cayleygraph/cayley/internal/http"
)

func NewHTTPCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "http",
		Short: "Serve an HTTP endpoint on the given host and port.",
		RunE: func(cmd *cobra.Command, args []string) error {
			printBackendInfo()
			p := mustSetupProfile(cmd)
			defer mustFinishProfile(p)

			h, err := openForQueries(cmd)
			if err != nil {
				return err
			}
			defer h.Close()

			err = chttp.SetupRoutes(h, &chttp.Config{
				Timeout:  viper.GetDuration(keyQueryTimeout),
				ReadOnly: viper.GetBool(KeyReadOnly),
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
	registerLoadFlags(cmd)
	viper.BindPFlag(keyQueryTimeout, cmd.Flags().Lookup("timeout"))
	return cmd
}
