# Install Cayley

## Install Cayley on Ubuntu

```
snap install --edge --devmode cayley
```

## Install Cayley on macOS

### Install Homebrew

macOS does not include the Homebrew brew package by default. Install brew using the [official instructions](https://brew.sh/#install)

### Install Cayley

```bash
brew install cayley
```

## Install Cayley with Docker

```bash
docker run -p 64210:64210 cayleygraph/cayley
```

For more information see [Container Documentation](Container.md)

## Build from Source

See instructions in [Contributing](Contributing.md)
