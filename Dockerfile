FROM openjdk:11

ARG ENTRYPOINT
ENV ENTRYPOINT=$ENTRYPOINT

LABEL org.opencontainers.image.description "Docker image for Aerospike's Graph Database, Firefly."
LABEL org.opencontainers.image.source = "https://github.com/citrusleaf/firefly"
ENV TINKERPOP_VERSION='3.6.0'
ENV MAVEN_VERSION='3.8.6'
ENV GREMLIN_CONSOLE_URL="https://dlcdn.apache.org/tinkerpop/$TINKERPOP_VERSION/apache-tinkerpop-gremlin-console-$TINKERPOP_VERSION-bin.zip"
ENV GREMLIN_SERVER_URL="https://dlcdn.apache.org/tinkerpop/$TINKERPOP_VERSION/apache-tinkerpop-gremlin-server-$TINKERPOP_VERSION-bin.zip"
ENV MAVEN_URL="https://dlcdn.apache.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"

RUN cd /tmp &&\
  curl -L -o maven.tar.gz $MAVEN_URL &&\
  curl -L -o gremlin-console.zip $GREMLIN_CONSOLE_URL &&\
  curl -L -o gremlin-server.zip $GREMLIN_SERVER_URL &&\
  tar -zxvf maven.tar.gz -C /opt/ &&\
  unzip -qq gremlin-console.zip -d /opt/ && ln -sf /opt/apache-tinkerpop-gremlin-console-$TINKERPOP_VERSION /opt/gremlin-console &&\
  unzip -qq gremlin-server.zip -d /opt/ && ln -sf /opt/apache-tinkerpop-gremlin-server-$TINKERPOP_VERSION /opt/gremlin-server
ENV PATH="$PATH:/opt/apache-maven-$MAVEN_VERSION/bin:/opt/gremlin-console/bin:/opt/gremlin-server/bin"
ADD . /opt/aerospike-firefly
WORKDIR /opt/aerospike-firefly
RUN mvn -DskipTests clean install --no-transfer-progress
RUN gremlin.sh -e scripts/console-setup.groovy &&\
    gremlin.sh -e scripts/console-plugin-enable.groovy &&\
    gremlin-server.sh install 'com.aerospike firefly-gremlin 0.3.0-SNAPSHOT'
RUN useradd -m firefly
USER firefly
ENTRYPOINT ["gremlin.sh"]
CMD ["-C"]
