#!/bin/bash

# This is used by the Dockerfile. It is not intended to be run directly.

# Inject classpath. Without this gremlin-server doesn't load all the appropriate jars for the bulk loader.
export CLASSPATH=$(cat /opt/classpath.txt)

# Need to generate JAVA_OPTIONS for gremlin-server. This is assigned in the gremlin-server script.
python3 /opt/scripts/generate_java_options.py

# Enable deep reflection for Java 17 and configure JVM memory
export JAVA_OPTIONS=$(cat /opt/scripts/java_options.txt)

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
  # A little bit of ascii art to make things fancy.
  echo "  ___                           _ _           _____                 _       _____                 _           "
  echo " / _ \\                         (_| |         |  __ \\               | |     /  ___|               (_)          "
  echo "/ /_\\ \\ ___ _ __ ___  ___ _ __  _| | __ ___  | |  \\/_ __ __ _ _ __ | |__   \\ \`--.  ___ _ ____   ___  ___ ___  "
  echo "|  _  |/ _ | '__/ _ \\/ __| '_ \\| \| |/ / _ \\ | | __| '__/ _\` | '_ \\| '_ \\   \`--. \\/ _ | '__\\ \\ / | |/ __/ _ \\ "
  echo "| | | |  __| | | (_) \\__ | |_) | |    |  __/ | |_\\ | | | (_| | |_) | | | | /\\__/ |  __| |   \\ V /| | (_|  __/"
  echo "\\_| |_/\\___|_|  \\___/|___| .__/|_|_|\\_\\\\___|  \\____|_|  \\__,_| .__/|_| |_| \\____/ \\___|_|    \\_/ |_|\\___\\___|"
  echo "                         | |                                | |                                             "
  echo "                         |_|                                |_|                                             "

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
    cp $CONF_DIR/firefly-gremlin-server-custom.yaml /opt/aerospike-firefly/conf/firefly-gremlin-server.yaml
    gremlin-server.sh $CONF_DIR/firefly-gremlin-server-custom.yaml

  # Else use the default server yaml and properties
  else
    echo "==> Docker image is using default firefly-graph.properties <=="
    echo "==== firefly-gremlin-server.yaml ===="
    cp $CONF_DIR/firefly-graph.properties /opt/aerospike-firefly/conf/firefly-graph.properties
    cp $CONF_DIR/firefly-gremlin-server-custom.yaml /opt/aerospike-firefly/conf/firefly-gremlin-server.yaml
    gremlin-server.sh $CONF_DIR/firefly-gremlin-server.yaml
  fi
) <&0 &
child_pid=$!
CLASSPATH="/opt/gremlin-server/ext/aerospike-graph-gremlin/lib/*" gremlin.sh -e scripts/warmup.groovy 2>&1 > /tmp/warmup.log
touch /tmp/firefly-ready
# Sit here until we get a signal at which point we go to stop_gremlin_server().
until wait; do :; done
