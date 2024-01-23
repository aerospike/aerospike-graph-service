FROM aerospike-graph-squash:latest

# Set input arguments.
ARG ENTRYPOINT
ENV ENTRYPOINT=$ENTRYPOINT

# Set container labels.
LABEL org.opencontainers.image.description = "Docker image for Aerospike Graph."
LABEL org.opencontainers.image.source = "https://github.com/citrusleaf/firefly"

# Set environment variables.
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
