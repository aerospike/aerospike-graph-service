FROM eclipse-temurin:17-jre-alpine AS build

# Set input arguments.
ARG FIREFLY_GRAPH
ARG BULKLOADER

ENV SPARK_VERSION='3.4.1'
ENV SPARK_URL="https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz"

# Set environment variables.
ENV CONF_DIR="/opt/conf"

RUN apk add --no-cache --update \
    curl \
    xz \
    tar \
    bash \
    dos2unix

COPY ${FIREFLY_GRAPH} /opt/firefly-graph.jar
COPY ${BULKLOADER} /opt/bulk-loader.jar
RUN curl -L -o /opt/spark.tgz $SPARK_URL &&\
    tar zxvf /opt/spark.tgz -C /opt/ &&\
    mv /opt/spark-$SPARK_VERSION-bin-hadoop3/jars /opt/spark

# remove spark archive and unused pieces
RUN rm -f /opt/spark.tgz && rm -rf /opt/spark-$SPARK_VERSION-bin-hadoop3

# Remove additional conflicting logger jars from spark.
RUN rm -f /opt/spark/slf4j-* && rm -f /opt/spark/commons-logging*

# Add scripts to container.
COPY docker-runtime-scripts /opt/scripts
# For Windows users fix for line endings
RUN dos2unix /opt/scripts/gremlin-server-docker.sh /opt/scripts/firefly-server.sh

RUN apk del xz tar curl dos2unix

COPY conf/docker-default/flattened-default-gremlin-server.yaml $CONF_DIR/flattened-default-gremlin-server.yaml

# Add firefly logback config to container.
COPY conf/logback.xml $CONF_DIR/logback.xml

FROM eclipse-temurin:17-jre-alpine AS main

# Set environment variables.
ENV CONF_DIR="/opt/conf"
ENV OUTPUT_SERVER_YAML="$CONF_DIR/aerospike-graph-service.yaml"

COPY --from=build /opt /opt

RUN apk add --no-cache --update \
    bash \
    python3

WORKDIR /opt

# Make gremlin-server-docker.sh runnable and make files in config dir read/write/executable.
RUN chmod +x scripts/gremlin-server-docker.sh
RUN chmod +x scripts/firefly-server.sh
RUN chmod -R 777 $CONF_DIR

HEALTHCHECK CMD ls /tmp/firefly-ready

ENTRYPOINT ["scripts/gremlin-server-docker.sh"]
