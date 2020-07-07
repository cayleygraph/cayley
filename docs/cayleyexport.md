# `cayleyexport`

```
cayleyexport <file>
```

## Synopsis

The `cayleyexport` tool exports content from a Cayley deployment.

See the [`cayleyimport`](cayleyimport.md) document for more information regarding [`cayleyimport`](cayleyimport.md), which provides the inverse “importing” capability.

Run `cayleyexport` from the system command line, not the Cayley shell.

## Arguments

## Options

### `--help`

Returns information on the options and use of **cayleyexport**.

### `--quiet`

Runs **cayleyexport** in a quiet mode that attempts to limit the amount of output.

### `--uri=<connectionString>`

Specify a resolvable URI connection string (enclose in quotes) to connect to the Cayley deployment.

```
--uri "http://host[:port]"
```

### `--format=<format>`

Format to use for the exported data (if can not be detected defaults to JSON-LD)

### `--out=<filename>`

Specifies the location and name of a file to export the data to. If you do not specify a file, **cayleyexport** writes data to the standard output (e.g. “stdout”).
