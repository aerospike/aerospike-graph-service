FROM openjdk:11

ARG ENTRYPOINT
ENV ENTRYPOINT=$ENTRYPOINT

ENV TINKERPOP_VERSION='3.6.0'
ENV MAVEN_VERSION='3.8.5'
ENV GREMLIN_CONSOLE_URL="https://dlcdn.apache.org/tinkerpop/3.6.0/apache-tinkerpop-gremlin-console-$TINKERPOP_VERSION-bin.zip"
ENV GREMLIN_SERVER_URL="https://dlcdn.apache.org/tinkerpop/3.6.0/apache-tinkerpop-gremlin-server-$TINKERPOP_VERSION-bin.zip"
ENV MAVEN_URL="https://dlcdn.apache.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"

RUN cd /tmp &&\
  curl -L -o maven.tar.gz $MAVEN_URL &&\
  curl -L -o gremlin-console.zip $GREMLIN_CONSOLE_URL &&\
  curl -L -o gremlin-server.zip $GREMLIN_SERVER_URL &&\
  tar -zxvf maven.tar.gz -C /opt/ &&\
  unzip gremlin-console.zip -d /opt/ && ln -sf /opt/apache-tinkerpop-gremlin-console-$TINKERPOP_VERSION /opt/gremlin-console &&\
  unzip gremlin-server.zip -d /opt/ && ln -sf /opt/apache-tinkerpop-gremlin-server-$TINKERPOP_VERSION /opt/gremlin-server
ENV PATH="$PATH:/opt/apache-maven-$MAVEN_VERSION/bin:/opt/gremlin-console/bin:/opt/gremlin-server/bin"
ADD . /opt/aerospike-firefly
WORKDIR /opt/aerospike-firefly
RUN mvn -DskipTests clean install
RUN gremlin.sh -e samples/console-setup.groovy &&\
    gremlin.sh -e samples/console-plugin-enable.groovy &&\
    gremlin-server.sh install 'com.aerospike firefly-gremlin 0.0.1-SNAPSHOT'
RUN useradd -m firefly
USER firefly
ENTRYPOINT ["gremlin.sh"]
CMD ["-i","samples/console-env-startup.groovy"]
