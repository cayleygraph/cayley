First, you'll need Go [(version 1.3.x or greater)](https://golang.org/doc/install) and a Go workspace. This is outlined by the Go team at http://golang.org/doc/code.html and is sort of the official way of going about it.

If you just want to build Cayley and check out the source, or use it as a library, a simple `go get github.com/google/cayley` will work!

But suppose you want to contribute back on your own fork (and pull requests are welcome!). A good way to do this is to set up your $GOPATH and then...

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
go get github.com/tools/godep
godep restore
go build ./cmd/cayley && ./cayley <subcommand> <your options>
```

Which will also resolve the relevant static content paths for serving HTTP.
