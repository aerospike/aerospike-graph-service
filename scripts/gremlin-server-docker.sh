#!/bin/bash

# This is used by the Dockerfile. It is not intended to be run directly.

# Create trap that redirects a CTRL-C even into the stop_gremlin_server function.
trap 'stop_gremlin_server' INT
stop_gremlin_server() {
    echo "\nStopping Gremlin Server"

    # Send kill signal to all spawned processes
    kill -s SIGTERM "${child_pid}" > /dev/null 2>&1
    return 0
}

# Launch gremlin-server in the background and sets the child_pid to the PID of the process.
(
  python3 $CONF_DIR/firefly-graph-configure.py $CONF_DIR/firefly-graph.properties
  gremlin-server.sh $CONF_DIR/firefly-gremlin-server.yaml
) <&0 &
child_pid=$!

# Sit here until we get a signal at which point we go to stop_gremlin_server().
until wait; do :; done
