# `cayleyimport`

```
cayleyimport <file>
```

## Synopsis

The `cayleyimport` tool imports content created by [`cayleyexport`](cayleyexport.md), or potentially, another third-party export tool.

See the [`cayleyexport`](cayleyexport.md) document for more information regarding [`cayleyexport`](cayleyexport.md), which provides the inverse “exporting” capability.

Run `cayleyimport` from the system command line, not the Cayley shell.

## Arguments

### `file`

Specifies the location and name of a file containing the data to import. If you do not specify a file, **cayleyimport** reads data from standard input (e.g. “stdin”).

## Options

### `--help`

Returns information on the options and use of **cayleyimport**.

### `--quiet`

Runs **cayleyimport** in a quiet mode that attempts to limit the amount of output.

### `--uri=<connectionString>`

Specify a resolvable URI connection string (enclose in quotes) to connect to the Cayley deployment.

```
--uri "http://host[:port]"
```

### `--format=<format>`

Format of the provided data (if can not be detected defaults to JSON-LD)
