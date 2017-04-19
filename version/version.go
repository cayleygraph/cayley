package version

var (
	Version = "0.7.0-alpha"

	// git hash should be filled by:
	// 	go build -ldflags="-X github.com/cayleygraph/cayley/version.GitHash=xxxx"

	GitHash   = "dev snapshot"
	BuildDate string
)
