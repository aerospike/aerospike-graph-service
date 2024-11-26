FROM alpine AS build

# Set input arguments.
ARG FIREFLY_GRAPH
ARG BULKLOADER
ARG SPARK_ZIP
ARG SPARK_VERSION

# Set environment variables.
ENV CONF_DIR="/opt/conf"

RUN apk add --no-cache --update \
    xz \
    tar \
    bash \
    dos2unix

COPY ${FIREFLY_GRAPH} /opt/firefly-graph.jar
COPY ${BULKLOADER} /opt/bulk-loader.jar
COPY ${SPARK_ZIP} /opt/spark.tgz
COPY conf/logback.xml ${CONF_DIR}/logback.xml
COPY conf/docker-default/flattened-default-gremlin-server.yaml ${CONF_DIR}/flattened-default-gremlin-server.yaml
COPY docker-runtime-scripts /opt/scripts

RUN echo "extracting spark archives"
RUN tar zxvf /opt/spark.tgz -C /opt/ &&\
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3/jars /opt/spark

# remove spark archive and unused pieces
RUN rm -f /opt/spark.tgz && rm -rf /opt/spark-${SPARK_VERSION}-bin-hadoop3

# Remove additional conflicting logger jars from spark.
RUN rm -f /opt/spark/slf4j-* && rm -f /opt/spark/commons-logging*

RUN apk del xz tar

WORKDIR /opt

# For Windows users fix for line endings
RUN dos2unix /opt/scripts/gremlin-server-docker.sh /opt/scripts/firefly-server.sh

FROM alpine AS main

# Set environment variables.
ENV CONF_DIR="/opt/conf"
ENV OUTPUT_SERVER_YAML="$CONF_DIR/aerospike-graph-service.yaml"
ENV HEALTHCHECK_FILE="/tmp/firefly-ready"

COPY --from=build /opt /opt

RUN apk add --no-cache --update bash openjdk17-jdk python3

WORKDIR /opt

# Make gremlin-server-docker.sh runnable and make files in config dir read/write/executable.
RUN chmod +x scripts/gremlin-server-docker.sh
RUN chmod +x scripts/firefly-server.sh
RUN chmod -R 777 $CONF_DIR

HEALTHCHECK CMD ls $HEALTHCHECK_FILE

ENTRYPOINT ["scripts/gremlin-server-docker.sh"]
