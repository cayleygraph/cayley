FROM golang:latest
MAINTAINER Barak Michener <me@barakmich.com>

# Set up workdir
WORKDIR /go/src/github.com/google/cayley

# Restore vendored dependencies
RUN go get github.com/tools/godep
ADD Godeps /go/src/github.com/google/cayley/Godeps
RUN godep restore

# Add and install cayley
ADD . .
RUN go install -v github.com/google/cayley

# Expose the port and volume for configuration and data persistence. If you're
# using a backend like bolt, make sure the file is saved to this directory.
VOLUME ["/data"]
EXPOSE 64321

CMD ["cayley", "http", "-config", "/data/cayley.cfg", "-init"]
