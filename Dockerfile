FROM golang:latest as builder

# Set up workdir
WORKDIR /go/src/github.com/cayleygraph/cayley

# Restore vendored dependencies
RUN sh -c "curl https://glide.sh/get | sh"
COPY glide.* ./
RUN glide install

# Create filesystem for minimal image
RUN mkdir -p /fs/assets
RUN mkdir -p /fs/bin
RUN mkdir -p /fs/data

# Copy CA certs from builder image to the filesystem of the cayley image
RUN mkdir -p /fs/etc/ssl/certs
RUN cp /etc/ssl/certs/ca-certificates.crt /fs/etc/ssl/certs/ca-certificates.crt

RUN mkdir -p /fs/lib/x86_64-linux-gnu
RUN cp /lib/x86_64-linux-gnu/libc-2.24.so /fs/lib/x86_64-linux-gnu/libc-2.24.so

# Add assets to target fs
COPY docs /fs/assets/docs
COPY static /fs/assets/static
COPY templates /fs/assets/templates

# Add and build static linked version of cayley
# This will show warnings that glibc is required at runtime which can be ignored
COPY . .
RUN go build \
  -ldflags="-linkmode external -extldflags -static -X github.com/cayleygraph/cayley/version.GitHash=$(git rev-parse HEAD | cut -c1-12)" \
  -a \
  -installsuffix cgo \
  -o /fs/bin/cayley \
  -v \
  ./cmd/cayley

RUN echo '{"store":{"backend":"bolt","address":"/fs/data/cayley.db"}}' > /etc/cayley.json
RUN /fs/bin/cayley init --config /etc/cayley.json


FROM scratch
MAINTAINER Barak Michener <me@barakmich.com>

# Expose the port and volume for configuration and data persistence. If you're
# using a backend like bolt, make sure the file is saved to this directory.
COPY --from=builder /fs /
VOLUME ["/data"]

EXPOSE 64210

ENTRYPOINT ["/bin/cayley", "http", "--assets", "/assets", "--host", ":64210"]
