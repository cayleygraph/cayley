package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/boltdb/bolt"
	"github.com/codegangsta/cli"
)

var branch, commit string

func main() {
	log.SetFlags(0)
	NewApp().Run(os.Args)
}

// NewApp creates an Application instance.
func NewApp() *cli.App {
	app := cli.NewApp()
	app.Name = "bolt"
	app.Usage = "BoltDB toolkit"
	app.Version = fmt.Sprintf("0.1.0 (%s %s)", branch, commit)
	app.Commands = []cli.Command{
		{
			Name:  "info",
			Usage: "Print basic information about a database",
			Action: func(c *cli.Context) {
				path := c.Args().Get(0)
				Info(path)
			},
		},
		{
			Name:  "get",
			Usage: "Retrieve a value for given key in a bucket",
			Action: func(c *cli.Context) {
				path, name, key := c.Args().Get(0), c.Args().Get(1), c.Args().Get(2)
				Get(path, name, key)
			},
		},
		{
			Name:  "keys",
			Usage: "Retrieve a list of all keys in a bucket",
			Action: func(c *cli.Context) {
				path, name := c.Args().Get(0), c.Args().Get(1)
				Keys(path, name)
			},
		},
		{
			Name:  "buckets",
			Usage: "Retrieves a list of all buckets",
			Action: func(c *cli.Context) {
				path := c.Args().Get(0)
				Buckets(path)
			},
		},
		{
			Name:  "pages",
			Usage: "Dumps page information for a database",
			Action: func(c *cli.Context) {
				path := c.Args().Get(0)
				Pages(path)
			},
		},
		{
			Name:  "check",
			Usage: "Performs a consistency check on the database",
			Action: func(c *cli.Context) {
				path := c.Args().Get(0)
				Check(path)
			},
		},
		{
			Name:  "stats",
			Usage: "Aggregate statistics for all buckets matching specified prefix",
			Action: func(c *cli.Context) {
				path, name := c.Args().Get(0), c.Args().Get(1)
				Stats(path, name)
			},
		},
		{
			Name:  "bench",
			Usage: "Performs a synthetic benchmark",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "profile-mode", Value: "rw", Usage: "Profile mode"},
				&cli.StringFlag{Name: "write-mode", Value: "seq", Usage: "Write mode"},
				&cli.StringFlag{Name: "read-mode", Value: "seq", Usage: "Read mode"},
				&cli.IntFlag{Name: "count", Value: 1000, Usage: "Item count"},
				&cli.IntFlag{Name: "batch-size", Usage: "Write batch size"},
				&cli.IntFlag{Name: "key-size", Value: 8, Usage: "Key size"},
				&cli.IntFlag{Name: "value-size", Value: 32, Usage: "Value size"},
				&cli.StringFlag{Name: "cpuprofile", Usage: "CPU profile output path"},
				&cli.StringFlag{Name: "memprofile", Usage: "Memory profile output path"},
				&cli.StringFlag{Name: "blockprofile", Usage: "Block profile output path"},
				&cli.StringFlag{Name: "stats-interval", Value: "0s", Usage: "Continuous stats interval"},
				&cli.Float64Flag{Name: "fill-percent", Value: bolt.DefaultFillPercent, Usage: "Fill percentage"},
				&cli.BoolFlag{Name: "no-sync", Usage: "Skip fsync on every commit"},
				&cli.BoolFlag{Name: "work", Usage: "Print the temp db and do not delete on exit"},
				&cli.StringFlag{Name: "path", Usage: "Path to database to use"},
			},
			Action: func(c *cli.Context) {
				statsInterval, err := time.ParseDuration(c.String("stats-interval"))
				if err != nil {
					fatal(err)
				}

				Bench(&BenchOptions{
					ProfileMode:   c.String("profile-mode"),
					WriteMode:     c.String("write-mode"),
					ReadMode:      c.String("read-mode"),
					Iterations:    c.Int("count"),
					BatchSize:     c.Int("batch-size"),
					KeySize:       c.Int("key-size"),
					ValueSize:     c.Int("value-size"),
					CPUProfile:    c.String("cpuprofile"),
					MemProfile:    c.String("memprofile"),
					BlockProfile:  c.String("blockprofile"),
					StatsInterval: statsInterval,
					FillPercent:   c.Float64("fill-percent"),
					NoSync:        c.Bool("no-sync"),
					Clean:         !c.Bool("work"),
					Path:          c.String("path"),
				})
			},
		}}
	return app
}

var logger = log.New(os.Stderr, "", 0)
var logBuffer *bytes.Buffer

func print(v ...interface{}) {
	if testMode {
		logger.Print(v...)
	} else {
		fmt.Print(v...)
	}
}

func printf(format string, v ...interface{}) {
	if testMode {
		logger.Printf(format, v...)
	} else {
		fmt.Printf(format, v...)
	}
}

func println(v ...interface{}) {
	if testMode {
		logger.Println(v...)
	} else {
		fmt.Println(v...)
	}
}

func fatal(v ...interface{}) {
	logger.Print(v...)
	if !testMode {
		os.Exit(1)
	}
}

func fatalf(format string, v ...interface{}) {
	logger.Printf(format, v...)
	if !testMode {
		os.Exit(1)
	}
}

func fatalln(v ...interface{}) {
	logger.Println(v...)
	if !testMode {
		os.Exit(1)
	}
}

// LogBuffer returns the contents of the log.
// This only works while the CLI is in test mode.
func LogBuffer() string {
	if logBuffer != nil {
		return logBuffer.String()
	}
	return ""
}

var testMode bool

// SetTestMode sets whether the CLI is running in test mode and resets the logger.
func SetTestMode(value bool) {
	testMode = value
	if testMode {
		logBuffer = bytes.NewBuffer(nil)
		logger = log.New(logBuffer, "", 0)
	} else {
		logger = log.New(os.Stderr, "", 0)
	}
}
