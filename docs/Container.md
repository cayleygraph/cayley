# Docker Container

## General Use Container

*This container is for general use. It is approximately 40MB.*

### Build the Container

*You don't need to build the container.*

```
cd <cayley source directory>
docker build -t sugarush/cayley .
```

### Run the Container

To run the container:

```
docker run -it --rm sugarush/cayley <options/flags>
```

### HTTP Interface

If you intend to use the HTTP interface, you must include the _-assets /assets_ flag. Your command may look something like:

```
docker run -it --rm sugarush/cayley http -assets /assets <...>
```

## Quickstart Container

*This container is not for general use. It is approximately 500MB.*

*You do not need to set the -assets flag when using the quickstart container.*

The quickstart container is configured to serve 30kmoviedata with BoltDB as a backend.

### Build the Container

*You don't need to build the container.*

Make sure to build the general container first.

```
cd <cayley source directory>
docker build -f Dockerfile-quickstart -t sugarush/cayley-quickstart .
```

### Run the Container

To start a quickstart container prepopulated with 30kmoviedata, run:

```
docker run -it --rm -p 64210:64210 sugarush/cayley-quickstart
```

The database will be available at http://localhost:64210.

### Docker Machine

If you are using Docker Machine, on a Mac for example, your database will be available at the Docker hosts's IP. This can be found by running:

```
docker-machine env <your docker machine>
```

You'll then connect to your database at http://&lt;docker-machine ip&gt;:64210.

### Persisting Data

To persist data, the first step is to obtain the BoltDB data and Cayley configuration file.

```
mkdir data
docker run -it --rm -v $(pwd)/data:/tmp --entrypoint /bin/sh sugarush/cayley-quickstart
cp -a /data/* /tmp
exit
```

Then start the container with your data directory mounted.

```
docker run -it --rm -v $(pwd)/data:/data sugarush/cayley-quickstart
```
