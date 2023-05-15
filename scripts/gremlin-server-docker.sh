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
  # If they passed in a server yaml
  if [ -e /opt/aerospike-firefly/conf/firefly-gremlin-server.yaml ]
  then
    echo "==> Docker image is using custom firefly-gremlin-server.yaml <=="
    echo "==== firefly-gremlin-server.yaml ===="
    cat /opt/aerospike-firefly/conf/firefly-gremlin-server.yaml
    gremlin-server.sh /opt/aerospike-firefly/conf/firefly-gremlin-server.yaml

  # Else if they passed only a properties file
  elif [ -e /opt/aerospike-firefly/conf/firefly-graph.properties ]
  then
    echo "==> Docker image is using custom firefly-graph.properties <=="
    echo "==== firefly-gremlin-server.yaml ===="
    cat $CONF_DIR/firefly-gremlin-server-custom.yaml
    gremlin-server.sh $CONF_DIR/firefly-gremlin-server-custom.yaml

  # Else use the default server yaml and properties
  else
    echo "==> Docker image is using default firefly-graph.properties <=="
    echo "==== firefly-gremlin-server.yaml ===="
    python3 $CONF_DIR/firefly-graph-configure.py $CONF_DIR/firefly-graph.properties
    cat $CONF_DIR/firefly-gremlin-server.yaml
    gremlin-server.sh $CONF_DIR/firefly-gremlin-server.yaml
  fi
) <&0 &
child_pid=$!
CLASSPATH="/opt/gremlin-server/ext/firefly-gremlin/lib/*" gremlin.sh -e scripts/warmup.groovy 2>&1 > /tmp/warmup.log
touch /tmp/firefly-ready
# Sit here until we get a signal at which point we go to stop_gremlin_server().
until wait; do :; done
