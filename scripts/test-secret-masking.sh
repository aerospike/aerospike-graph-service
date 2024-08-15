#!/usr/bin/bash

set -eo pipefail

echo "Searching for instances of masked secret..."
count=$(grep -o -c simon <(docker logs firefly) || true)
echo "Search complete..."
echo "Found $count instances of masked secret."
exit $count
