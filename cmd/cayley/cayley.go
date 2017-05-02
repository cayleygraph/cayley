// Copyright 2016 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build !appengine

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/codelingo/cayley/cmd/cayley/command"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/codelingo/cayley/clog"
	_ "github.com/codelingo/cayley/clog/glog"
	"github.com/codelingo/cayley/graph"
	"github.com/codelingo/cayley/quad"
	"github.com/codelingo/cayley/version"

	// Load supported backends
	_ "github.com/codelingo/cayley/graph/bolt"
	_ "github.com/codelingo/cayley/graph/leveldb"
	_ "github.com/codelingo/cayley/graph/memstore"
	_ "github.com/codelingo/cayley/graph/mongo"
	_ "github.com/codelingo/cayley/graph/sql"

	// Load all supported quad formats.
	_ "github.com/codelingo/cayley/quad/dot"
	_ "github.com/codelingo/cayley/quad/gml"
	_ "github.com/codelingo/cayley/quad/graphml"
	_ "github.com/codelingo/cayley/quad/json"
	_ "github.com/codelingo/cayley/quad/jsonld"
	_ "github.com/codelingo/cayley/quad/nquads"
	_ "github.com/codelingo/cayley/quad/pquads"

	// Load writer registry
	_ "github.com/codelingo/cayley/writer"

	// Load supported query languages
	_ "github.com/codelingo/cayley/query/gizmo"
	_ "github.com/codelingo/cayley/query/graphql"
	_ "github.com/codelingo/cayley/query/mql"
	_ "github.com/codelingo/cayley/query/sexp"
)

var (
	rootCmd = &cobra.Command{
		Use:   "cayley",
		Short: "Cayley is a graph store and graph query layer.",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			clog.Infof("Cayley version: %s (%s)", version.Version, version.GitHash)
			if conf, _ := cmd.Flags().GetString("config"); conf != "" {
				viper.SetConfigFile(conf)
			}
			err := viper.ReadInConfig()
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok && err != nil {
				return err
			}
			if conf := viper.ConfigFileUsed(); conf != "" {
				wd, _ := os.Getwd()
				if rel, _ := filepath.Rel(wd, conf); rel != "" && strings.Count(rel, "..") < 3 {
					conf = rel
				}
				clog.Infof("using config file: %s", conf)
			}
			// force viper to load flags to variables
			graph.IgnoreDuplicates = viper.GetBool("load.ignore_duplicates")
			graph.IgnoreMissing = viper.GetBool("load.ignore_missing")
			quad.DefaultBatch = viper.GetInt("load.batch")
			return nil
		},
	}
	versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Prints the version of Cayley.",
		// do not execute any persistent actions
		PersistentPreRun: func(cmd *cobra.Command, args []string) {},
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Cayley version:", version.Version)
			fmt.Println("Git commit hash:", version.GitHash)
			if version.BuildDate != "" {
				fmt.Println("Build date:", version.BuildDate)
			}
		},
	}
)

type pFlag struct {
	flag.Value
}

func (pFlag) Type() string { return "string" }

func init() {
	// set config names and paths
	viper.SetConfigName("cayley")
	viper.SetEnvPrefix("cayley")
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.cayley/")
	viper.AddConfigPath("/etc/")
	if conf := os.Getenv("CAYLEY_CFG"); conf != "" {
		viper.SetConfigFile(conf)
	}

	rootCmd.AddCommand(
		versionCmd,
		command.NewInitDatabaseCmd(),
		command.NewLoadDatabaseCmd(),
		command.NewDumpDatabaseCmd(),
		command.NewUpgradeCmd(),
		command.NewReplCmd(),
		command.NewHttpCmd(),
		command.NewConvertCmd(),
		command.NewDedupCommand(),
	)
	rootCmd.PersistentFlags().StringP("config", "c", "", "path to an explicit configuration file")

	rootCmd.PersistentFlags().StringP("db", "d", "memstore", "database backend to use")
	rootCmd.PersistentFlags().StringP("dbpath", "a", "", "path or address string for database")
	rootCmd.PersistentFlags().Bool("read_only", false, "open database in read-only mode")

	rootCmd.PersistentFlags().Bool("dup", true, "don't stop loading on duplicated on add")
	rootCmd.PersistentFlags().Bool("missing", false, "don't stop loading on missing key on delete")
	rootCmd.PersistentFlags().Int("batch", quad.DefaultBatch, "size of quads batch to load at once")

	// bind flags to config variables
	viper.BindPFlag(command.KeyBackend, rootCmd.PersistentFlags().Lookup("db"))
	viper.BindPFlag(command.KeyAddress, rootCmd.PersistentFlags().Lookup("dbpath"))
	viper.BindPFlag(command.KeyReadOnly, rootCmd.PersistentFlags().Lookup("read_only"))
	viper.BindPFlag("load.ignore_duplicates", rootCmd.PersistentFlags().Lookup("dup"))
	viper.BindPFlag("load.ignore_missing", rootCmd.PersistentFlags().Lookup("missing"))
	viper.BindPFlag(command.KeyLoadBatch, rootCmd.PersistentFlags().Lookup("batch"))

	// make both store.path and store.address work
	viper.RegisterAlias(command.KeyPath, command.KeyAddress)
	// aliases for legacy config files
	viper.RegisterAlias("database", command.KeyBackend)
	viper.RegisterAlias("db_path", command.KeyAddress)
	viper.RegisterAlias("read_only", command.KeyReadOnly)
	viper.RegisterAlias("db_options", command.KeyOptions)

	{ // re-register standard Go flags to cobra
		rf := rootCmd.PersistentFlags()
		flag.CommandLine.VisitAll(func(f *flag.Flag) {
			switch f.Name {
			case "v": // glog.v
				rf.VarP(pFlag{f.Value}, "verbose", "v", f.Usage)
			case "vmodule": // glog.vmodule
				rf.Var(pFlag{f.Value}, "vmodule", f.Usage)
			case "log_backtrace_at": // glog.log_backtrace_at
				rf.Var(pFlag{f.Value}, "backtrace", f.Usage)
			case "stderrthreshold": // glog.stderrthreshold
				rf.VarP(pFlag{f.Value}, "log", "l", f.Usage)
			case "alsologtostderr": // glog.alsologtostderr
				rf.Var(pFlag{f.Value}, f.Name, f.Usage)
			case "logtostderr": // glog.logtostderr
				rf.Var(pFlag{f.Value}, f.Name, f.Usage)
			case "log_dir": // glog.log_dir
				rf.Var(pFlag{f.Value}, "logs", f.Usage)
			}
		})
		// make sure flags parsed flag is set - parse empty args
		flag.CommandLine = flag.NewFlagSet("", flag.ContinueOnError)
		flag.CommandLine.Parse([]string{""})
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		clog.Errorf("%v", err)
		os.Exit(1)
	}
}
