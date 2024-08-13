#!/usr/bin/bash

set -eo pipefail

count=$(grep -o simon <(docker logs firefly) | wc -l)
echo "Found $count instances of masked secret."
exit $count
