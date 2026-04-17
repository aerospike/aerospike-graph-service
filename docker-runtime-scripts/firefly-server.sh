#!/bin/bash
# Copyright 2022-2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

[[ -n "$DEBUG" ]] && set -x

# absolute file path requires 'file:'
LOGBACK_CONF="file:/opt/conf/logback.xml"

# Find Java
if [[ "$JAVA_HOME" = "" ]] ; then
    JAVA="java"
else
    JAVA="$JAVA_HOME/bin/java"
fi

# Set Java options
if [[ "$JAVA_OPTIONS" = "" ]] ; then
    JAVA_OPTIONS="-Xms512m -XX:MaxRAMPercentage=80.0"
fi

FIREFLY_SERVER_CMD=com.aerospike.firefly.runtime.FireflyServer
GREMLIN_YAML=$1
echo starting with GREMLIN_YAML = $GREMLIN_YAML

#:/opt/olap.jar
CLASSPATH="/opt/firefly-graph.jar:/opt/bulk-loader.jar:/opt/spark/*"

# Create trap that redirects signal into the stop_ags function.
stop_ags() {
    echo "Stopping AGS ${java_pid}"

    # Send kill signal to all spawned processes
    kill -s SIGTERM "${java_pid}" 2>&1 # > /dev/null
    return 0
}
trap stop_ags INT
trap stop_ags TERM

(
  $JAVA -Dlogback.configurationFile=$LOGBACK_CONF $JAVA_OPTIONS -cp $CLASSPATH $FIREFLY_SERVER_CMD "$GREMLIN_YAML"
) <&0 &
java_pid=$!

# Sit here until we get a signal at which point we go to stop_ags().
until wait; do :; done
