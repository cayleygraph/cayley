FROM golang:1.13 as builder

# Create filesystem for minimal image
WORKDIR /fs

RUN mkdir -p etc/ssl/certs lib/x86_64-linux-gnu tmp bin assets data; \
    # Copy CA Certificates
    cp /etc/ssl/certs/ca-certificates.crt etc/ssl/certs/ca-certificates.crt; \
    # Copy C standard library
    cp /lib/x86_64-linux-gnu/libc-* lib/x86_64-linux-gnu/

# Set up workdir for compiling
WORKDIR /src

# Copy dependencies and install first
COPY go.mod go.sum ./
RUN go mod download

# Add all the other files
ADD . .

# Pass a Git short SHA as build information to be used for displaying version
RUN SHORT_SHA=$(git rev-parse --short=12 HEAD); \
    go build \
    -ldflags="-linkmode external -extldflags -static -X github.com/cayleygraph/cayley/version.GitHash=$SHORT_SHA" \
    -a \
    -installsuffix cgo \
    -o /fs/bin/cayley \
    -v \
    ./cmd/cayley

# Move assets into the filesystem
RUN mv docs static templates /fs/assets; \
    # Move persisted configuration into filesystem
    mv configurations/persisted.json /fs/etc/cayley.json

WORKDIR /fs

# Initialize bolt indexes file
RUN ./bin/cayley init --config etc/cayley.json

FROM scratch

# Copy filesystem as root
COPY --from=builder /fs /

# Define volume for configuration and data persistence. If you're using a
# backend like bolt, make sure the file is saved to this directory.
VOLUME [ "/data" ]

EXPOSE 64210

# Adding everything to entrypoint allows us to init, load and serve only with
# arguments passed to docker run. For example:
# `docker run cayleygraph/cayley --init -i /data/my_data.nq`
ENTRYPOINT ["cayley", "http", "--assets=/assets", "--host=:64210"]
