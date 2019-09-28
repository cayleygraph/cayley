FROM golang:1.13.1 as builder

# Set up workdir to make go mod work
WORKDIR /cayley

# Install dependencies first
COPY go.mod go.sum ./
RUN go mod download

# Create filesystem for minimal image
RUN mkdir -p /fs/etc/ssl/certs /fs/lib/x86_64-linux-gnu /fs/tmp /fs/bin /fs/assets; \
    # Copy CA Certificates
    cp /etc/ssl/certs/ca-certificates.crt /fs/etc/ssl/certs/ca-certificates.crt; \
    # Copy C standard library
    cp /lib/x86_64-linux-gnu/libc-* /fs/lib/x86_64-linux-gnu/

ADD . .

# Move assets into the filesystem
RUN mv docs static templates /fs/assets

RUN SHORT_SHA=$(git rev-parse --short=12 HEAD); \
    go build \
    -ldflags="-linkmode external -extldflags -static -X github.com/cayleygraph/cayley/version.GitHash=$SHORT_SHA" \
    -a \
    -installsuffix cgo \
    -o /fs/bin/cayley \
    -v \
    ./cmd/cayley

FROM scratch

COPY --from=builder /fs /

EXPOSE 64210

ENTRYPOINT ["cayley", "http", "--assets=/assets", "--host=:64210"]