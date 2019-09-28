# Community Involvement

Join our community on [discourse.cayley.io](https://discourse.cayley.io) or other [Locations](Locations.md).

# Simply building Cayley

If your version of Go < 1.13, you need to run:

```
export GO111MODULE=on
```

Follow the instructions for running Cayley locally:

```
# clone project
git clone https://github.com/cayleygraph/cayley
cd cayley

# download dependencies
go mod download

# build the binary
go build ./cmd/cayley

# try the generated binary
./cayley help
```

Give it a quick test with:

```
./cayley repl -i data/testdata.nq
```

To run the web frontend, replace the "repl" command with "http"
```
./cayley http -i data/testdata.nq
```

You can now open the WebUI in your browser: http://127.0.0.1:64210

# Hacking on Cayley

First, you'll need Go [(version 1.11.x or greater)](https://golang.org/doc/install) and a Go workspace.
This is outlined by the Go team at http://golang.org/doc/code.html and is sort of the official way of going about it.

If your version of Go < 1.13, you need to run:

```
export GO111MODULE=on
```

If you just want to build Cayley and check out the source, or use it as a library, a simple `go get github.com/cayleygraph/cayley` will work!

But suppose you want to contribute back on your own fork (and pull requests are welcome!).
A good way to do this is to set up your $GOPATH and then...

```
mkdir -p $GOPATH/src/github.com/cayleygraph
cd $GOPATH/src/github.com/cayleygraph
git clone https://github.com/$GITHUBUSERNAME/cayley
```

...where $GITHUBUSERNAME is, well, your GitHub username :) You'll probably want to add

```
cd cayley
git remote add upstream http://github.com/cayleygraph/cayley
```

So that you can keep up with the latest changes by periodically running

```
git pull --rebase upstream
```

With that in place, that folder will reflect your local fork, be able to take changes from the official fork, and build in the Go style.

For iterating, it can be helpful to, from the directory, run

```
go build ./cmd/cayley && ./cayley <subcommand> <your options>
```

Which will also resolve the relevant static content paths for serving HTTP.

**Reminder:** add yourself to CONTRIBUTORS and AUTHORS.

# Running Unit Tests

If your version of Go < 1.13, you need to run:

```
export GO111MODULE=on
```

First, `cd` into the `cayley` project folder and run:
```
go test ./...
```

If you have a Docker installed, you can also run tests for remote backend implementations:
```
go test -tags docker ./...
```

If you have a Docker installed, you only want to run tests for a specific backend implementations eg. mongodb
```
go test -tags docker ./graph/nosql/mongo
```

Integration tests can be enabled with environment variable:
```
RUN_INTEGRATION=true go test ./...
```
