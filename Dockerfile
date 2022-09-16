FROM openjdk:11

# Set input arguments.
ARG AEROSPIKE_HOST
ENV AEROSPIKE_HOST=$AEROSPIKE_HOST
ARG ENTRYPOINT
ENV ENTRYPOINT=$ENTRYPOINT

# Set container labels.
LABEL org.opencontainers.image.description = "Docker image for Aerospike's graph database, Firefly."
LABEL org.opencontainers.image.source = "https://github.com/citrusleaf/firefly"

# Set environment variables.
ENV TINKERPOP_VERSION='3.6.0'
ENV MAVEN_VERSION='3.8.6'
ENV GREMLIN_CONSOLE_URL="https://dlcdn.apache.org/tinkerpop/$TINKERPOP_VERSION/apache-tinkerpop-gremlin-console-$TINKERPOP_VERSION-bin.zip"
ENV GREMLIN_SERVER_URL="https://dlcdn.apache.org/tinkerpop/$TINKERPOP_VERSION/apache-tinkerpop-gremlin-server-$TINKERPOP_VERSION-bin.zip"
ENV MAVEN_URL="https://dlcdn.apache.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"
ENV AIR_ROUTES_50K_URL="https://raw.githubusercontent.com/krlawrence/graph/master/sample-data/air-routes-latest.graphml"
ENV CONF_DIR="/opt/aerospike-firefly/conf/docker-default"

# Download air-routes, maven, gremlin-console, gremlin-server, and move/unzip/untar them.
RUN cd /tmp &&\
  curl -L -o air-routes-50k.graphml $AIR_ROUTES_50K_URL &&\
  curl -L -o maven.tar.gz $MAVEN_URL &&\
  curl -L -o gremlin-console.zip $GREMLIN_CONSOLE_URL &&\
  curl -L -o gremlin-server.zip $GREMLIN_SERVER_URL &&\
  mkdir /opt/air-routes &&\
  mv air-routes-50k.graphml /opt/air-routes/ &&\
  tar -zxvf maven.tar.gz -C /opt/ &&\
  unzip -qq gremlin-console.zip -d /opt/ && ln -sf /opt/apache-tinkerpop-gremlin-console-$TINKERPOP_VERSION /opt/gremlin-console &&\
  unzip -qq gremlin-server.zip -d /opt/ && ln -sf /opt/apache-tinkerpop-gremlin-server-$TINKERPOP_VERSION /opt/gremlin-server

# Append to PATH for maven/console.
ENV PATH="$PATH:/opt/apache-maven-$MAVEN_VERSION/bin:/opt/gremlin-console/bin:/opt/gremlin-server/bin"

# Add current direction to /opt/aerospike-firefly and set working directory.
ADD . /opt/aerospike-firefly
WORKDIR /opt/aerospike-firefly

# Make gremlin-server-docker.sh runnable and make files in config dir read/write/executable.
RUN chmod +x scripts/gremlin-server-docker.sh
RUN chmod -R 777 $CONF_DIR

# Install vi and python interpretter.
RUN apt-get update
RUN apt-get install -y vim
RUN apt-get install -y python3

# Build Firefly.
RUN mvn -DskipTests clean install --no-transfer-progress

# Setup gremlin console and gremlin-server. Install firefly in gremlin-server.
RUN gremlin.sh -e scripts/console-setup.groovy &&\
    gremlin.sh -e scripts/console-plugin-enable.groovy &&\
    gremlin-server.sh install 'com.aerospike firefly-gremlin 0.3.0-SNAPSHOT'

# Add user firefly and set user to firefly.
RUN useradd -m firefly
USER firefly

# Entry point, run script.
ENTRYPOINT ["scripts/gremlin-server-docker.sh"]
