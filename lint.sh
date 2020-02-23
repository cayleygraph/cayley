#!/bin/bash

# Lint all the files using go lint excluding errors which are currently explicitly ignore
# This script is intended to be used in the continuous integration process
# When editing, it is highly recommended to use ShellCheck (https://www.shellcheck.net/) to avoid common pitfalls

# Patterns to be ignored from the go lint output
IGNORED_PATTERNS=(
    " comment "
    "graph\/proto\/primitive.pb.go"
    "func name will be used as refs.RefsOf by other packages, and that stutters; consider calling this Of"
    "method CapitalizedUri should be CapitalizedURI"
    "func name will be used as path.PathFromIterator by other packages, and that stutters; consider calling this FromIterator"
)

# Patterns joined into a regular expression
REGEX=$(printf "|(%s)" "${IGNORED_PATTERNS[@]}")
REGEX=${REGEX:1}

# Execute go lint on all the files and filter output by the regualr expression
output=$( ( (golint ./... | egrep -v "$REGEX") 2>&1 ) | tee /dev/fd/2);
if [ -z "$output" ]
then
    exit 0
else
    exit 1
fi