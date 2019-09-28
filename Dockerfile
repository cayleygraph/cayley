FROM golang:1.13 as builder

# Set up workdir to make go mod work
WORKDIR /cayley

# Install dependencies first
COPY go.mod go.sum ./
RUN go mod download

# Create filesystem for minimal image
RUN mkdir -p /fs/etc/ssl/certs /fs/lib/x86_64-linux-gnu /fs/tmp /fs/bin /fs/assets /fs/data; \
    # Copy CA Certificates
    cp /etc/ssl/certs/ca-certificates.crt /fs/etc/ssl/certs/ca-certificates.crt; \
    # Copy C standard library
    cp /lib/x86_64-linux-gnu/libc-* /fs/lib/x86_64-linux-gnu/

ADD . .

# Move assets into the filesystem
RUN mv docs static templates /fs/assets; \
    # Move persisted configuration into filesystem
    mv configurations/persisted.json /fs/etc/cayley.json

# Pass a Git short SHA as build information to be used for displaying version
RUN SHORT_SHA=$(git rev-parse --short=12 HEAD); \
    go build \
    -ldflags="-linkmode external -extldflags -static -X github.com/cayleygraph/cayley/version.GitHash=$SHORT_SHA" \
    -a \
    -installsuffix cgo \
    -o /fs/bin/cayley \
    -v \
    ./cmd/cayley

# Initialize bolt indexes file
RUN /fs/bin/cayley init --config /fs/etc/cayley.json

FROM scratch

COPY --from=builder /fs /

# Define volume for configuration and data persistence. If you're using a
# backend like bolt, make sure the file is saved to this directory.
VOLUME [ "/data" ]

EXPOSE 64210

# Adding everything to entrypoint allows us to init, load and serve only with
# arguments passed to docker run. For example:
# `docker run cayleygraph/cayley --init -i /data/my_data.nq`
ENTRYPOINT ["cayley", "http", "--assets=/assets", "--host=:64210"]
