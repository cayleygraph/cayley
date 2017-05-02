FROM golang:latest
MAINTAINER Barak Michener <me@barakmich.com>

# Set up workdir
WORKDIR /go/src/github.com/codelingo/cayley

# Restore vendored dependencies
RUN sh -c "curl https://glide.sh/get | sh"
ADD glide.* ./
RUN glide install

# Add and install cayley
ADD . .
RUN go install -ldflags="-X github.com/codelingo/cayley/version.GitHash=$(git rev-parse HEAD | cut -c1-12)" -v ./cmd/cayley

# Expose the port and volume for configuration and data persistence. If you're
# using a backend like bolt, make sure the file is saved to this directory.
RUN mkdir /data
#VOLUME ["/data"]

EXPOSE 64210

RUN echo '{"store":{"backend":"bolt","address":"/data/cayley.db"}}' > /etc/cayley.json
RUN cayley init --config /etc/cayley.json

ENTRYPOINT ["cayley", "http", "--host", ":64210"]
