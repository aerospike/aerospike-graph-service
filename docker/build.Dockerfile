FROM amazoncorretto:11

# Set input arguments.
ARG RELEASE_BUILD
ENV RELEASE_BUILD=$RELEASE_BUILD
ARG ENTRYPOINT
ENV ENTRYPOINT=$ENTRYPOINT

# Set environment variables.
ENV TINKERPOP_VERSION='3.6.3'
ENV MAVEN_VERSION='3.8.8'
ENV JANSI_VERSION='2.4.0'
ENV GREMLIN_CONSOLE_URL="https://dlcdn.apache.org/tinkerpop/$TINKERPOP_VERSION/apache-tinkerpop-gremlin-console-$TINKERPOP_VERSION-bin.zip"
ENV GREMLIN_SERVER_URL="https://dlcdn.apache.org/tinkerpop/$TINKERPOP_VERSION/apache-tinkerpop-gremlin-server-$TINKERPOP_VERSION-bin.zip"
ENV JANSI_URL="https://repo1.maven.org/maven2/org/fusesource/jansi/jansi/$JANSI_VERSION/jansi-$JANSI_VERSION.jar"
ENV MAVEN_URL="https://dlcdn.apache.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"
ENV CONF_DIR="/opt/aerospike-firefly/conf/docker-default"

# Install things required to create image.
RUN yum -y update &&\
    yum -y install xz &&\
    yum -y install tar &&\
    yum -y install python3 &&\
    yum -y install unzip &&\
    yum -y install util-linux

# Download maven, gremlin-console, gremlin-server, and move/unzip/untar them.
RUN cd /tmp &&\
  curl -L -o maven.tar.gz $MAVEN_URL &&\
  curl -L -o gremlin-console.zip $GREMLIN_CONSOLE_URL &&\
  curl -L -o gremlin-server.zip $GREMLIN_SERVER_URL &&\
  curl -L -o jansi-$JANSI_VERSION.jar $JANSI_URL &&\
  tar -zxvf maven.tar.gz -C /opt/ &&\
  unzip -qq gremlin-console.zip -d /opt/ && ln -sf /opt/apache-tinkerpop-gremlin-console-$TINKERPOP_VERSION /opt/gremlin-console &&\
  unzip -qq gremlin-server.zip -d /opt/ && ln -sf /opt/apache-tinkerpop-gremlin-server-$TINKERPOP_VERSION /opt/gremlin-server &&\
  mv jansi-$JANSI_VERSION.jar /opt/gremlin-console/lib && \
  rm -rf gremlin-console.zip &&\
  rm -rf gremlin-server.zip &&\
  rm -rf maven.tar.gz

# Append to PATH for maven/console.
ENV PATH="$PATH:/opt/apache-maven-$MAVEN_VERSION/bin:/opt/gremlin-console/bin:/opt/gremlin-server/bin"

# Add docker-default and scripts to docker container.
ADD . /opt/aerospike-firefly
WORKDIR /opt/aerospike-firefly

# Build Firefly.
RUN mvn -pl aerospike-graph-gremlin -am -Dmaven.test.skip=true -DskipTests=true -Dmaven.test.skip.exec=true clean install --no-transfer-progress

# Setup gremlin console and gremlin-server. Install firefly in gremlin-server.
# If RELEASE_BUILD is set, then use release build, otherwise use SNAPSHOT build.
RUN \
    if [[ $RELEASE_BUILD -eq "1" ]] ;  \
    then gremlin-server.sh install 'com.aerospike aerospike-graph-gremlin 1.1.0' ;  \
    else gremlin-server.sh install 'com.aerospike aerospike-graph-gremlin 1.1.0-SNAPSHOT' ;  \
    fi

# Remove source code.
RUN cd .. && rm -rf /opt/aerospike-firefly

# Remove extra packages
RUN yum remove -y vim-minimal vim-data python3 unzip xz tar

# Add scripts and conf to container.
ADD conf/docker-default /opt/aerospike-firefly/conf/docker-default
ADD scripts /opt/aerospike-firefly/scripts

# Make gremlin-server-docker.sh runnable and make files in config dir read/write/executable.
RUN chmod +x scripts/gremlin-server-docker.sh
RUN chmod -R 777 $CONF_DIR

# Add user firefly.
RUN useradd -m firefly

# Make firefly owner of conf dir.
RUN chown firefly:firefly -R /opt/aerospike-firefly/conf/

RUN rm -rf /root/.m2
