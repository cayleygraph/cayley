---
description: >-
  Linked Data has multiple representations. The Cayley CLI includes a utility to
  convert Linked Data files from one format to another.
---

# Convert Linked Data files

## Convert from one format to another

```
$ cayley convert -i data.jsonld -o data.nquads
```

`-i` is the input file to be converted. In this example it is a [JSON-LD](https://www.w3.org/TR/json-ld11/) file named `data.jsonld`.

`-o` is the file to be created in the desired format. In this example it is a [N-Quads](https://www.w3.org/TR/n-quads/) file named `data.nquads`.

### Explicitly specify formats

The formats of the input and output files are detected automatically by the file extension. In case a specific format should be used for input or output use `--load_format` and `--dump_format` respectively.

```text
$ cayley convet -i data.jsonld -o data --dump_format pquads
```

`--dump_format` is set to the P-Quads format, a binary format used internally in Cayley.

