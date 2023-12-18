#!/bin/bash

set -e

root_dir="$(dirname "$0")/.."

for dir in "$root_dir"/*; do
    [ ! -f "$dir/pyproject.toml" ] && continue

    pushd "$dir" && poetry run task lint && popd
done
