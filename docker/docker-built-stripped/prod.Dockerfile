FROM aerospike-graph-stripped-squash:latest

# Set input arguments.
ARG ENTRYPOINT
ENV ENTRYPOINT=$ENTRYPOINT

# Set container labels.
LABEL org.opencontainers.image.description = "Stripped Docker image for Aerospike Graph (Bulk loader and spark omitted)."
LABEL org.opencontainers.image.source = "https://github.com/citrusleaf/firefly"

# Set environment variables.
ENV TINKERPOP_VERSION='3.7.1'
ENV MAVEN_VERSION='3.8.8'
ENV JANSI_VERSION='2.4.0'
ENV GREMLIN_CONSOLE_URL="https://archive.apache.org/dist/tinkerpop/$TINKERPOP_VERSION/apache-tinkerpop-gremlin-console-$TINKERPOP_VERSION-bin.zip"
ENV GREMLIN_SERVER_URL="https://archive.apache.org/dist/tinkerpop/$TINKERPOP_VERSION/apache-tinkerpop-gremlin-server-$TINKERPOP_VERSION-bin.zip"
ENV JANSI_URL="https://repo1.maven.org/maven2/org/fusesource/jansi/jansi/$JANSI_VERSION/jansi-$JANSI_VERSION.jar"
ENV MAVEN_URL="https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"
ENV AIR_ROUTES_50K_URL="https://raw.githubusercontent.com/krlawrence/graph/master/sample-data/air-routes-latest.graphml"
ENV CONF_DIR="/opt/conf"
ENV OUTPUT_SERVER_YAML="$CONF_DIR/aerospike-graph-service.yaml"

# Append to PATH for maven/console.
ENV PATH="$PATH:/opt/apache-maven-$MAVEN_VERSION/bin:/opt/gremlin-console/bin:/opt/gremlin-server/bin"

# Set user to firefly.
USER firefly

HEALTHCHECK CMD ls /tmp/firefly-ready

WORKDIR /opt/aerospike-graph

# Entry point, run script.
ENTRYPOINT ["scripts/gremlin-server-docker.sh"]
