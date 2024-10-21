#!/bin/bash

# This is used by the Dockerfile. It is not intended to be run directly.

# Generate properties file and gremlin-server yaml file.
# Inputs are: input_properties_file_path, input_yaml_file_path, output_properties_file_path, output_yaml_file_path, output_java_options_file_path.
# Note, input_properties_file_path is the same as output_properties_file_path, we just override it as we complete the config.
# Also we run this script regardless of whether or not the user supplied a custom file. This is because we want to get the memory
# configurations from the environment variables.
python3 scripts/configure_aerospike_graph.py "/opt/aerospike-graph/aerospike-graph.properties" \
    "$CONF_DIR/flattened-default-gremlin-server.yaml" \
    "$OUTPUT_SERVER_YAML" \
    "$CONF_DIR" \
    "$CONF_DIR/java_options.txt"

if [[ $? != 0 ]]; then
  echo "Failed to configure aerospike graph"
  exit 1
fi

# Exit if the python script failed.
if [ $? != 0 ];
then
    echo "Failed to configure Aerospike Graph Service. Exiting."
    exit 1
fi

# Set the java options.
export JAVA_OPTIONS=$(cat "$CONF_DIR/java_options.txt")

# Configuration complete.
echo "Successfully configured Aerospike Graph Service."

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

  # Bootstrap gremlin-server.
  # If they passed in a server yaml
  if [ -e /opt/aerospike-graph/custom/aerospike-graph-service.yaml ]
  then
    # This is a precautionary override, just in case a customer really needs to.
    # If they are using this they are on their own linking yaml->properties, but
    # can still set the min heap and max heap via environment variables and it will work.
    echo "==== Bootstrapping Aerospike Graph Service with custom gremlin-server.yaml. ===="
    export GREMLIN_SERVER_YAML_PATH=/opt/aerospike-graph/custom/aerospike-graph-service.yaml

  # Else if they passed only a properties file
  else
    echo "==== Bootstrapping Aerospike Graph Service with generated gremlin-server.yaml. ===="
    export GREMLIN_SERVER_YAML_PATH=$OUTPUT_SERVER_YAML
  fi

  scripts/firefly-server.sh $GREMLIN_SERVER_YAML_PATH
) <&0 &
child_pid=$!
# !!! todo: WARMUP
touch /tmp/firefly-ready
# Sit here until we get a signal at which point we go to stop_gremlin_server().
until wait; do :; done
