# Running in a container

A container exposing the HTTP API of cayley is available.
To run the container one must first setup a data directory that contains the configuration file and optionally contains persistent files (i.e. a boltdb database file).

```
mkdir data
cp my_config.cfg data/cayley.cfg
docker run -v $PWD/data:/data -p 64321:64321 -d quay.io/barakmich/cayley
```
