# Running in a container

A container exposing the HTTP API of Cayley is available.

## Running with default configuration

Container is configured to serve 30kmoviedata with BoltDB as a backend.

```
docker run -p 64210:64210 -d quay.io/cayleygraph/cayley
```

Database will be available at http://localhost:64210.

## Custom configuration

To run the container one must first setup a data directory that contains the configuration file and optionally contains persistent files (i.e. a boltdb database file).

```
mkdir data
cp cayley_example.yml data/cayley.yml
cp data/testdata.nq data/my_data.nq
# initialize and serve database
docker run -v $PWD/data:/data -p 64210:64210 -d quay.io/cayleygraph/cayley --init -i /data/my_data.nq
# serve existing database
docker run -v $PWD/data:/data -p 64210:64210 -d quay.io/cayleygraph/cayley
```

## Other commands

Container runs `cayley http` command by default. To run any other Cayley command reset the entry point for container:
```
docker run -v $PWD/data:/data quay.io/cayleygraph/cayley --entrypoint=cayley version
```