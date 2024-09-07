FROM amazoncorretto:17

# Set input arguments.
ARG RELEASE_BUILD
ENV RELEASE_BUILD=$RELEASE_BUILD
ARG ENTRYPOINT
ENV ENTRYPOINT=$ENTRYPOINT

# Set environment variables.
ENV TINKERPOP_VERSION='3.7.1'
ENV MAVEN_VERSION='3.8.8'
ENV JANSI_VERSION='2.4.0'
ENV SPARK_VERSION='3.4.1'
ENV GREMLIN_CONSOLE_URL="https://archive.apache.org/dist/tinkerpop/$TINKERPOP_VERSION/apache-tinkerpop-gremlin-console-$TINKERPOP_VERSION-bin.zip"
ENV GREMLIN_SERVER_URL="https://archive.apache.org/dist/tinkerpop/$TINKERPOP_VERSION/apache-tinkerpop-gremlin-server-$TINKERPOP_VERSION-bin.zip"
ENV JANSI_URL="https://repo1.maven.org/maven2/org/fusesource/jansi/jansi/$JANSI_VERSION/jansi-$JANSI_VERSION.jar"
ENV MAVEN_URL="https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"
ENV CONF_DIR="/opt/conf"
ENV OUTPUT_SERVER_YAML="$CONF_DIR/aerospike-graph-service.yaml"
ENV SPARK_URL="https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz"

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
  mv jansi-$JANSI_VERSION.jar /opt/gremlin-console/lib &&\
  rm -rf gremlin-console.zip &&\
  rm -rf gremlin-server.zip &&\
  rm -rf maven.tar.gz

# Append to PATH for maven and server.
ENV PATH="$PATH:/opt/apache-maven-$MAVEN_VERSION/bin:/opt/gremlin-console/bin:/opt/gremlin-server/bin"

RUN curl -L -o /opt/spark.tgz $SPARK_URL &&\
    tar zxvf /opt/spark.tgz -C /opt/ &&\
    mv /opt/spark-$SPARK_VERSION-bin-hadoop3 /opt/spark

# Add docker-default and scripts to docker container.
ADD . /opt/aerospike-graph
WORKDIR /opt/aerospike-graph

# Build Firefly.
RUN mvn -pl aerospike-graph-gremlin -pl aerospike-graph-bulk-loader -pl aerospike-graph-sizing-tool -am -Dmaven.test.skip=true -DskipTests=true -Dmaven.test.skip.exec=true clean install --no-transfer-progress

# Move bulk-loader jar to /opt/bulk-loader.
RUN mkdir /opt/bulk-loader &&\
    mv /opt/aerospike-graph/aerospike-graph-bulk-loader/target/aerospike-graph-bulk-loader-2.4.0-SNAPSHOT.jar /opt/bulk-loader

# Move sizing-tool jar to /opt/sizing-tool.
RUN mkdir /opt/sizing-tool &&\
    mv /opt/aerospike-graph/aerospike-graph-sizing-tool/target/aerospike-graph-sizing-tool-2.4.0-SNAPSHOT.jar /opt/sizing-tool

# Build CLASSPATH before invoking gremlin-server. This is assigned in the gremlin-server script.
# Note bulk-loader also needs to be in the classpath.
RUN python3 docker-runtime-scripts/generate_classpath.py

RUN mkdir -p $CONF_DIR && mv /opt/aerospike-graph/conf/docker-default/flattened-default-gremlin-server.yaml $CONF_DIR/flattened-default-gremlin-server.yaml

# Setup gremlin-server. Install firefly in gremlin-server.
# If RELEASE_BUILD is set, then use release build, otherwise use SNAPSHOT build.
RUN \
    if [[ $RELEASE_BUILD -eq "1" ]] ;  \
    then gremlin-server.sh install 'com.aerospike aerospike-graph-gremlin 2.4.0-SNAPSHOT' ;  \
    else gremlin-server.sh install 'com.aerospike aerospike-graph-gremlin 2.4.0-SNAPSHOT' ;  \
    fi

# Remove source code.
RUN cd .. && rm -rf /opt/aerospike-graph

# Remove extra packages
RUN yum remove -y vim-minimal vim-data unzip xz tar

# Remove additional conflicting logger jars from spark.
RUN rm /opt/spark/jars/slf4j-* && rm /opt/spark/jars/commons-logging*

# Add scripts to container.
ADD docker-runtime-scripts /opt/aerospike-graph/scripts

# Make gremlin-server-docker.sh runnable and make files in config dir read/write/executable.
RUN chmod +x scripts/gremlin-server-docker.sh
RUN chmod -R 777 $CONF_DIR

# Add user firefly.
RUN useradd -m firefly

# Copy maven repo to firefly user.
RUN chown firefly:firefly -R /opt/spark && chown firefly:firefly -R /opt/bulk-loader && chown firefly:firefly -R /opt/sizing-tool

# Make firefly owner of conf dir.
RUN chown firefly:firefly -R $CONF_DIR

RUN rm -rf /root/.m2
