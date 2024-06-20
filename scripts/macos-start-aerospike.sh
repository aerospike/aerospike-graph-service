#!/usr/bin/env bash
docker run --workdir $(pwd) -t -i -v /var/run/docker.sock:/var/run/docker.sock -v $(pwd):$(pwd) -v/tmp:/tmp firefly:dev scripts/start_aerospike.sh
rm -rf .github/aerospike/venv/
