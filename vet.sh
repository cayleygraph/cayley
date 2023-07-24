#!/bin/bash

# Vet all the files using go vet excluding errors which are currently explicitly ignore
# This script is intended to be used in the continuous integration process
# When editing, it is highly recommended to use ShellCheck (https://www.shellcheck.net/) to avoid common pitfalls

# Patterns to be ignored from the go lint output
IGNORED_PATTERNS=(
    "^# "

    # Field order is well-defined
    "/quad\.Quad struct literal uses unkeyed fields"

    # Code imported from b
    " method Seek\(k int64\) .* should have signature "
)

# Patterns joined into a regular expression
REGEX=$(printf "|(%s)" "${IGNORED_PATTERNS[@]}")
REGEX=${REGEX:1}

# Execute go vet on all the files and filter output by the regualr expression
output=$( (go vet ./... 2>&1 | grep -Ev "$REGEX") | tee /dev/fd/2);
if [ -z "$output" ]
then
    exit 0
else
    exit 1
fi
