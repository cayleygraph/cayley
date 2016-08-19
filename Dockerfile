FROM golang:latest
MAINTAINER Barak Michener <me@barakmich.com>

# Set up workdir
WORKDIR /go/src/github.com/cayleygraph/cayley

# Restore vendored dependencies
RUN sh -c "curl https://glide.sh/get | sh"
ADD glide.* ./
RUN glide install

# Add and install cayley
ADD . .
RUN go install -v ./cmd/cayley

# Expose the port and volume for configuration and data persistence. If you're
# using a backend like bolt, make sure the file is saved to this directory.
RUN mkdir /data #VOLUME ["/data"]
EXPOSE 64210

RUN echo '{"database":"bolt","db_path":"/data/cayley","read_only":false}' > /data/cayley.cfg
RUN cayley init -db bolt -dbpath /data/cayley -quads ./data/30kmoviedata.nq.gz

ENTRYPOINT ["cayley", "http", "-config", "/data/cayley.cfg", "-host", "0.0.0.0", "-port", "64210"]
