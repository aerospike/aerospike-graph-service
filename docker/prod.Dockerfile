FROM aerospike-graph-squash:latest

# Set input arguments.
ARG RELEASE_BUILD
ENV RELEASE_BUILD=$RELEASE_BUILD
ARG AEROSPIKE_HOST
ENV AEROSPIKE_HOST=$AEROSPIKE_HOST
ARG ENTRYPOINT
ENV ENTRYPOINT=$ENTRYPOINT
ARG AEROSPIKE_PORT
ENV AEROSPIKE_PORT=$AEROSPIKE_PORT
ARG AEROSPIKE_NAMESPACE
ENV AEROSPIKE_NAMESPACE=$AEROSPIKE_NAMESPACE
ARG FIREFLY_DATA_MODEL
ENV FIREFLY_DATA_MODEL=$FIREFLY_DATA_MODEL

# Set container labels.
LABEL org.opencontainers.image.description = "Docker image for Aerospike's graph database, Firefly."
LABEL org.opencontainers.image.source = "https://github.com/citrusleaf/firefly"

# Set environment variables.
ENV TINKERPOP_VERSION='3.6.3'
ENV MAVEN_VERSION='3.8.8'
ENV JANSI_VERSION='2.4.0'
ENV GREMLIN_CONSOLE_URL="https://dlcdn.apache.org/tinkerpop/$TINKERPOP_VERSION/apache-tinkerpop-gremlin-console-$TINKERPOP_VERSION-bin.zip"
ENV GREMLIN_SERVER_URL="https://dlcdn.apache.org/tinkerpop/$TINKERPOP_VERSION/apache-tinkerpop-gremlin-server-$TINKERPOP_VERSION-bin.zip"
ENV JANSI_URL="https://repo1.maven.org/maven2/org/fusesource/jansi/jansi/$JANSI_VERSION/jansi-$JANSI_VERSION.jar"
ENV MAVEN_URL="https://dlcdn.apache.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"
ENV AIR_ROUTES_50K_URL="https://raw.githubusercontent.com/krlawrence/graph/master/sample-data/air-routes-latest.graphml"
ENV CONF_DIR="/opt/aerospike-firefly/conf/docker-default"

# Append to PATH for maven/console.
ENV PATH="$PATH:/opt/apache-maven-$MAVEN_VERSION/bin:/opt/gremlin-console/bin:/opt/gremlin-server/bin"

# Set user to firefly.
USER firefly

HEALTHCHECK CMD ls /tmp/firefly-ready

WORKDIR /opt/aerospike-firefly

# Entry point, run script.
ENTRYPOINT ["scripts/gremlin-server-docker.sh"]
