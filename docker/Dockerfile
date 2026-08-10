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

FROM alpine:3.23.5@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40 AS build

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

# Validate the JAR arguments before COPY consumes them. COPY silently collapses a glob
# matching two files into one, last writer wins, and expands an empty argument away
# entirely, so neither condition is detectable after the fact.
RUN set -eu; \
    for spec in "FIREFLY_GRAPH|${FIREFLY_GRAPH:-}" "BULKLOADER|${BULKLOADER:-}"; do \
      name="${spec%%|*}"; value="${spec#*|}"; \
      if [ -z "$value" ]; then \
        echo "ERROR: build arg $name is empty or unset. COPY would ingest the whole build context into a directory instead of a JAR." >&2; \
        exit 1; \
      fi; \
      case "$value" in \
        *[*?[]*) \
          echo "ERROR: build arg $name is a glob ($value). Pass exactly one explicit path; COPY collapses a multi-match into one file with no way to detect it afterwards." >&2; \
          exit 1; \
          ;; \
      esac; \
    done

COPY ${FIREFLY_GRAPH} /opt/firefly-graph.jar
COPY ${BULKLOADER} /opt/bulk-loader.jar
#COPY ${OLAP} /opt/olap.jar

# Trailing glob, not *.asc: it always matches the JAR itself, so the build still works when no
# detached signature was produced. A *.asc source that matches nothing fails the COPY outright.
COPY ${FIREFLY_GRAPH}* /opt/sig/graph/
COPY ${BULKLOADER}* /opt/sig/bulk/

# Every JAR argument must have resolved to exactly one readable JAR. Unconditional: no ARG,
# build-arg or environment variable can disable this. An unset or empty argument makes
# Docker expand the COPY above to `COPY  /opt/firefly-graph.jar`, which copies the entire
# build context into a DIRECTORY at that path and exits 0, producing an image that cannot
# load its entrypoint class while every build signal reports success.
RUN set -eu; \
    apk add --no-cache --virtual .jar-guard unzip; \
    for spec in \
      "/opt/firefly-graph.jar|com/aerospike/firefly/runtime/FireflyServer.class" \
      "/opt/bulk-loader.jar|"; do \
      path="${spec%%|*}"; entry="${spec#*|}"; \
      if [ -d "$path" ]; then \
        echo "ERROR: $path is a directory, not a JAR. Its build arg was empty or unset, so COPY ingested the build context." >&2; \
        exit 1; \
      fi; \
      if [ ! -f "$path" ] || [ ! -s "$path" ]; then \
        echo "ERROR: $path is missing, not a regular file, or empty." >&2; \
        exit 1; \
      fi; \
      if ! unzip -t "$path" >/dev/null 2>&1; then \
        echo "ERROR: $path is not a readable archive." >&2; \
        exit 1; \
      fi; \
      if [ -n "$entry" ] && ! unzip -l "$path" | grep -qF "$entry"; then \
        echo "ERROR: $path does not contain $entry." >&2; \
        exit 1; \
      fi; \
      echo "jar-guard OK: $path"; \
    done; \
    apk del .jar-guard

# Verify each JAR against its detached signature when both a signature and the public key are
# present. A missing signature or key warns and continues; an invalid signature fails the build.
RUN --mount=type=secret,id=gpg_public_key \
    set -eu; \
    key=/run/secrets/gpg_public_key; \
    if [ ! -s "$key" ]; then \
      echo "WARNING: no gpg_public_key build secret; JAR signatures not verified"; \
    else \
      apk add --no-cache --virtual .gpg-verify gnupg; \
      export GNUPGHOME=/tmp/gnupg; mkdir -p "$GNUPGHOME"; chmod 700 "$GNUPGHOME"; \
      gpg --batch --quiet --import "$key"; \
      for pair in "/opt/sig/graph|/opt/firefly-graph.jar" "/opt/sig/bulk|/opt/bulk-loader.jar"; do \
        dir="${pair%%|*}"; jar="${pair#*|}"; \
        sig="$(find "$dir" -maxdepth 1 -name '*.asc' -print -quit)"; \
        if [ -z "$sig" ]; then \
          echo "WARNING: no detached signature alongside $(basename "$jar"); not verified"; \
          continue; \
        fi; \
        if ! gpg --batch --verify "$sig" "$jar" 2>&1; then \
          echo "ERROR: signature $sig does not verify against $jar" >&2; \
          exit 1; \
        fi; \
        echo "signature OK: $jar"; \
      done; \
      rm -rf "$GNUPGHOME"; \
      apk del .gpg-verify; \
    fi; \
    rm -rf /opt/sig

COPY conf/logback.xml ${CONF_DIR}/logback.xml
COPY conf/docker-default/flattened-default-gremlin-server.yaml ${CONF_DIR}/flattened-default-gremlin-server.yaml
COPY docker-runtime-scripts/gremlin-server-docker.sh /opt/scripts/gremlin-server-docker.sh
COPY docker-runtime-scripts/firefly-server.sh /opt/scripts/firefly-server.sh
COPY docker-runtime-scripts/graph_config/configure_aerospike_graph.py /opt/scripts/graph_config/configure_aerospike_graph.py

RUN apk del xz tar

WORKDIR /opt

# For Windows users fix for line endings
RUN dos2unix /opt/scripts/gremlin-server-docker.sh /opt/scripts/firefly-server.sh

FROM alpine:3.23.5@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40 AS main

# Set environment variables.
ENV CONF_DIR="/opt/conf"
ENV OUTPUT_SERVER_YAML="$CONF_DIR/aerospike-graph-service.yaml"
ENV HEALTHCHECK_FILE="/tmp/firefly-ready"

# Create the user before COPY so ownership is set inline; a separate `chown -R /opt`
# would rewrite every file into a new layer, duplicating the JARs.
RUN addgroup -S firefly && adduser -S -G firefly firefly

COPY --from=build --chown=firefly:firefly /opt /opt

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

# Run as the non-root user created above.
USER firefly

# Document the served ports: Gremlin server and Prometheus metrics.
EXPOSE 8182 9090

HEALTHCHECK CMD ls $HEALTHCHECK_FILE

ENTRYPOINT ["scripts/gremlin-server-docker.sh"]
