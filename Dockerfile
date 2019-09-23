FROM golang:1.11 as builder

# Set up workdir
WORKDIR /cayley

# Restore dependencies
COPY go.mod go.sum ./
RUN go mod download

# This will be used to init cayley and as config file in the final image.
# Make sure you start every path with %PREFIX% to make it available in both 
# the builder image and the final image.
RUN echo '{"store":{"backend":"bolt","address":"%PREFIX%/data/cayley.db"}}' > config.json

# Create filesystem for minimal image
RUN mkdir -p /fs/assets
RUN mkdir -p /fs/bin
RUN mkdir -p /fs/data
RUN mkdir -p /fs/etc
RUN sed 's_%PREFIX%__g' config.json > /fs/etc/cayley.json

ENV PATH /fs/bin:$PATH

# Copy CA certs from builder image to the filesystem of the cayley image
RUN mkdir -p /fs/etc/ssl/certs
RUN cp /etc/ssl/certs/ca-certificates.crt /fs/etc/ssl/certs/ca-certificates.crt

RUN mkdir -p /fs/lib/x86_64-linux-gnu
RUN cp /lib/x86_64-linux-gnu/libc-* /fs/lib/x86_64-linux-gnu/

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

RUN sed 's_%PREFIX%_/fs_g' config.json > /etc/cayley.json
RUN cayley init --config /etc/cayley.json


FROM scratch
LABEL maintainer="Yannic Bonenberger" \
           email="contact@yannic-bonenberger.com"

# Expose the port and volume for configuration and data persistence. If you're
# using a backend like bolt, make sure the file is saved to this directory.
COPY --from=builder /fs /
VOLUME ["/data"]

EXPOSE 64210

# Adding everything to entrypoint allows us to init+load+serve
# with default containers parameters:
#   i.e.: `docker run cayleygraph/cayley --init -i /data/my_data.nq`
ENTRYPOINT ["cayley", "http", "--assets", "/assets", "--host", ":64210"]
