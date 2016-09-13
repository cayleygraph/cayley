# Running in a container

A container exposing the HTTP API of cayley is available.

## Running with default configuration

Container is configured to serve 30kmoviedata with BoltDB as a backend.

```
docker run -p 64210:64210 -d quay.io/codelingo/cayley
```

Database will be available at http://localhost:64210.

## Custom configuration

To run the container one must first setup a data directory that contains the configuration file and optionally contains persistent files (i.e. a boltdb database file).

```
mkdir data
cp my_config.cfg data/cayley.cfg
cp my_data.nq data/my_data.nq
# initialize and serve database
docker run -v $PWD/data:/data -p 64210:64210 -d quay.io/codelingo/cayley -init -quads /data/my_data.nq
# serve existing database
docker run -v $PWD/data:/data -p 64210:64210 -d quay.io/codelingo/cayley
```
