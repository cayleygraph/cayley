# About appengine

This directory contains a appengine deployable cayley graph instance.

# Running/testing traditional appengine locally

Install the latest appengine sdk, and ensure that your local version of go
matches the same version obtained from `goapp`.

Note: if you use a more recent version of Go locally, `goapp` get might
accidentally copy the wrong files from the local `$GOPATH`.

```sh
$ cd $GOPATH/src/github.com/cayleygraph/cayley/appengine

# check go version
$ go version

# check goapp version
$ goapp version

# install dependencies
$ goapp get

# ensure that goapp can build
$ goapp build

# test locally
$ goapp serve
```

# Running/testing flexible appengine locally

The latest appengine flexible environment can be tested instead of the
traditional environment by doing the following:

```sh
# calls ./sync-assets.sh
$ go generate

$ goapp serve app.flexible.yaml
```

# Deploying to traditional appengine environment
```sh
$ goapp deploy -version 1 -application <MY_PROJECT_ID>
```

# Deploying to flexible appengine environment
```sh
# install aedeploy if not installed
$ go get -u google.golang.org/appengine/cmd/aedeploy

# calls ./sync-assets.sh
$ go generate

# deploy
$ aedeploy gcloud app deploy app.flexible.yaml
```
