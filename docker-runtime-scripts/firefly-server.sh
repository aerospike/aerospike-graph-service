#!/bin/bash

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
    JAVA_OPTIONS="-Xms512m -Xmx4096m"
fi

FIREFLY_SERVER_CMD=com.aerospike.firefly.runtime.FireflyServer
GREMLIN_YAML=$1
echo starting with GREMLIN_YAML = $GREMLIN_YAML

CLASSPATH="/opt/firefly-graph.jar:/opt/bulk-loader.jar:/opt/spark/*"

$JAVA -Dlogback.configurationFile=$LOGBACK_CONF $JAVA_OPTIONS -cp $CLASSPATH $FIREFLY_SERVER_CMD "$GREMLIN_YAML"
exit 0
