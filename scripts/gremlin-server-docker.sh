#!/bin/bash

# This is used by the Dockerfile. It is not intended to be run directly.

python3 $CONF_DIR/firefly-graph-configure.py $CONF_DIR/firefly-graph.properties
gremlin-server.sh $CONF_DIR/firefly-gremlin-server.yaml