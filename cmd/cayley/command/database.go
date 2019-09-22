package command

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/internal"
	"github.com/cayleygraph/quad"
)

const (
	KeyBackend  = "store.backend"
	KeyAddress  = "store.address"
	KeyPath     = "store.path"
	KeyReadOnly = "store.read_only"
	KeyOptions  = "store.options"

	KeyLoadBatch = "load.batch"
)

const (
	flagLoad       = "load"
	flagLoadFormat = "load_format"
	flagDump       = "dump"
	flagDumpFormat = "dump_format"
)

var ErrNotPersistent = errors.New("database type is not persistent")

func registerLoadFlags(cmd *cobra.Command) {
	// TODO: allow to load multiple files
	cmd.Flags().StringP(flagLoad, "i", "", `quad file to load after initialization (".gz" supported, "-" for stdin)`)
	var names []string
	for _, f := range quad.Formats() {
		if f.Reader != nil {
			names = append(names, f.Name)
		}
	}
	sort.Strings(names)
	cmd.Flags().String(flagLoadFormat, "", `quad file format to use for loading instead of auto-detection ("`+strings.Join(names, `", "`)+`")`)
}

func registerDumpFlags(cmd *cobra.Command) {
	cmd.Flags().StringP(flagDump, "o", "", `quad file to dump the database to (".gz" supported, "-" for stdout)`)
	var names []string
	for _, f := range quad.Formats() {
		if f.Writer != nil {
			names = append(names, f.Name)
		}
	}
	sort.Strings(names)
	cmd.Flags().String(flagDumpFormat, "", `quad file format to use instead of auto-detection ("`+strings.Join(names, `", "`)+`")`)
}

func NewInitDatabaseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Create an empty database.",
		RunE: func(cmd *cobra.Command, args []string) error {
			printBackendInfo()
			name := viper.GetString(KeyBackend)
			if graph.IsRegistered(name) && !graph.IsPersistent(name) {
				return ErrNotPersistent
			}
			// TODO: maybe check read-only flag in config before that?
			if err := initDatabase(); err != nil {
				return err
			}
			return nil
		},
	}
	return cmd
}

func NewLoadDatabaseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "load",
		Short: "Bulk-load a quad file into the database.",
		RunE: func(cmd *cobra.Command, args []string) error {
			printBackendInfo()
			p := mustSetupProfile(cmd)
			defer mustFinishProfile(p)
			load, _ := cmd.Flags().GetString(flagLoad)
			if load == "" && len(args) == 1 {
				load = args[0]
			}
			if load == "" {
				return errors.New("one quads file must be specified")
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

			qw, err := h.NewQuadWriter()
			if err != nil {
				return err
			}
			defer qw.Close()

			// TODO: check read-only flag in config before that?
			typ, _ := cmd.Flags().GetString(flagLoadFormat)
			if err = internal.Load(qw, quad.DefaultBatch, load, typ); err != nil {
				return err
			}

			if dump, _ := cmd.Flags().GetString(flagDump); dump != "" {
				typ, _ := cmd.Flags().GetString(flagDumpFormat)
				if err = dumpDatabase(h, dump, typ); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().Bool("init", false, "initialize the database before using it")
	registerLoadFlags(cmd)
	registerDumpFlags(cmd)
	return cmd
}

func NewDumpDatabaseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dump",
		Short: "Bulk-dump the database into a quad file.",
		RunE: func(cmd *cobra.Command, args []string) error {
			printBackendInfo()
			dump, _ := cmd.Flags().GetString(flagDump)
			if dump == "" && len(args) == 1 {
				dump = args[0]
			}
			if dump == "" {
				dump = "-"
			}
			h, err := openDatabase()
			if err != nil {
				return err
			}
			defer h.Close()

			typ, _ := cmd.Flags().GetString(flagDumpFormat)
			return dumpDatabase(h, dump, typ)
		},
	}
	registerDumpFlags(cmd)
	return cmd
}

func NewUpgradeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade Cayley database to current supported format.",
		RunE: func(cmd *cobra.Command, args []string) error {
			printBackendInfo()
			name := viper.GetString(KeyBackend)
			if graph.IsRegistered(name) && !graph.IsPersistent(name) {
				return ErrNotPersistent
			}
			addr := viper.GetString(KeyAddress)
			opts := graph.Options(viper.GetStringMap(KeyOptions))
			clog.Infof("upgrading database...")
			return graph.UpgradeQuadStore(name, addr, opts)
		},
	}
	return cmd
}

func printBackendInfo() {
	name := viper.GetString(KeyBackend)
	path := viper.GetString(KeyAddress)
	if path != "" {
		path = " (" + path + ")"
	}
	clog.Infof("using backend %q%s", name, path)
}

func initDatabase() error {
	name := viper.GetString(KeyBackend)
	path := viper.GetString(KeyAddress)
	opts := viper.GetStringMap(KeyOptions)
	return graph.InitQuadStore(name, path, graph.Options(opts))
}

func openDatabase() (*graph.Handle, error) {
	name := viper.GetString(KeyBackend)
	path := viper.GetString(KeyAddress)
	opts := graph.Options(viper.GetStringMap(KeyOptions))
	qs, err := graph.NewQuadStore(name, path, opts)
	if err != nil {
		return nil, err
	}
	qw, err := graph.NewQuadWriter("single", qs, opts)
	if err != nil {
		return nil, err
	}
	return &graph.Handle{QuadStore: qs, QuadWriter: qw}, nil
}

func openForQueries(cmd *cobra.Command) (*graph.Handle, error) {
	if init, err := cmd.Flags().GetBool("init"); err != nil {
		return nil, err
	} else if init {
		if err = initDatabase(); err == graph.ErrDatabaseExists {
			clog.Infof("database already initialized, skipping init")
		} else if err != nil {
			return nil, err
		}
	}
	var load string
	h, err := openDatabase()
	if err == graph.ErrQuadStoreNotPersistent {
		load = viper.GetString(KeyAddress)
		viper.Set(KeyAddress, "")
		h, err = openDatabase()
	}
	if err == graph.ErrQuadStoreNotPersistent {
		return nil, fmt.Errorf("%v; did you mean -i flag?", err)
	} else if err != nil {
		return nil, err
	}

	if load2, _ := cmd.Flags().GetString(flagLoad); load2 != "" {
		if load != "" {
			h.Close()
			return nil, fmt.Errorf("both -a and -i flags cannot be specified")
		}
		load = load2
	}
	if load != "" {
		qw, err := h.NewQuadWriter()
		if err != nil {
			h.Close()
			return nil, err
		}
		defer qw.Close()

		typ, _ := cmd.Flags().GetString(flagLoadFormat)
		// TODO: check read-only flag in config before that?
		start := time.Now()
		if err = internal.Load(qw, quad.DefaultBatch, load, typ); err != nil {
			h.Close()
			return nil, err
		}
		clog.Infof("loaded %q in %v", load, time.Since(start))
	}
	return h, nil
}

type profileData struct {
	cpuProfile *os.File
	memPath    string
}

func mustSetupProfile(cmd *cobra.Command) profileData {
	p := profileData{}
	mpp := cmd.Flag("memprofile")
	p.memPath = mpp.Value.String()
	cpp := cmd.Flag("cpuprofile")
	v := cpp.Value.String()
	if v != "" {
		f, err := os.Create(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not open CPU profile file %s\n", v)
			os.Exit(1)
		}
		p.cpuProfile = f
		pprof.StartCPUProfile(f)
	}
	return p
}

func mustFinishProfile(p profileData) {
	if p.cpuProfile != nil {
		pprof.StopCPUProfile()
		p.cpuProfile.Close()
	}
	if p.memPath != "" {
		f, err := os.Create(p.memPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not open memory profile file %s\n", p.memPath)
			os.Exit(1)
		}
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "Could not write memory profile file %s\n", p.memPath)
		}
		f.Close()
	}
}
