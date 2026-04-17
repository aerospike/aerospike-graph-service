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

FROM alpine:latest AS build

# Set input arguments.
ARG FIREFLY_GRAPH
ARG BULKLOADER
#ARG OLAP

# Set environment variables.
ENV CONF_DIR="/opt/conf"

RUN apk add --no-cache --update \
    xz \
    tar \
    bash \
    dos2unix

COPY ${FIREFLY_GRAPH} /opt/firefly-graph.jar
COPY ${BULKLOADER} /opt/bulk-loader.jar
#COPY ${OLAP} /opt/olap.jar
COPY conf/logback.xml ${CONF_DIR}/logback.xml
COPY conf/docker-default/flattened-default-gremlin-server.yaml ${CONF_DIR}/flattened-default-gremlin-server.yaml
COPY docker-runtime-scripts/gremlin-server-docker.sh /opt/scripts/gremlin-server-docker.sh
COPY docker-runtime-scripts/firefly-server.sh /opt/scripts/firefly-server.sh
COPY docker-runtime-scripts/graph_config/configure_aerospike_graph.py /opt/scripts/graph_config/configure_aerospike_graph.py
COPY .git .git

RUN apk del xz tar

WORKDIR /opt

# For Windows users fix for line endings
RUN dos2unix /opt/scripts/gremlin-server-docker.sh /opt/scripts/firefly-server.sh

FROM alpine:latest AS main

# Set environment variables.
ENV CONF_DIR="/opt/conf"
ENV OUTPUT_SERVER_YAML="$CONF_DIR/aerospike-graph-service.yaml"
ENV HEALTHCHECK_FILE="/tmp/firefly-ready"

COPY --from=build /opt /opt

RUN apk upgrade --no-cache && \
    apk add --no-cache bash openjdk17-jdk python3 py3-pip openssl gcompat

# Enable glibc compatibility for netty-tcnative on ARM64
ENV LD_PRELOAD=/lib/libgcompat.so.0
RUN pip install pyyaml --break-system-packages

WORKDIR /opt

# Make gremlin-server-docker.sh runnable and make files in config dir read/write/executable.
RUN chmod +x scripts/gremlin-server-docker.sh
RUN chmod +x scripts/firefly-server.sh
RUN chmod -R 777 $CONF_DIR

HEALTHCHECK CMD ls $HEALTHCHECK_FILE

ENTRYPOINT ["scripts/gremlin-server-docker.sh"]
