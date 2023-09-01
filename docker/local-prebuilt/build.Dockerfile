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
ENV SPARK_VERSION='3.4.0'
ENV GREMLIN_CONSOLE_URL="https://archive.apache.org/dist/tinkerpop/$TINKERPOP_VERSION/apache-tinkerpop-gremlin-console-$TINKERPOP_VERSION-bin.zip"
ENV GREMLIN_SERVER_URL="https://archive.apache.org/dist/tinkerpop/$TINKERPOP_VERSION/apache-tinkerpop-gremlin-server-$TINKERPOP_VERSION-bin.zip"
ENV JANSI_URL="https://repo1.maven.org/maven2/org/fusesource/jansi/jansi/$JANSI_VERSION/jansi-$JANSI_VERSION.jar"
ENV MAVEN_URL="https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"
ENV CONF_DIR="/opt/aerospike-firefly/conf/docker-default"
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

# Add docker-default and scripts to docker container.
ADD .. /opt/aerospike-firefly
WORKDIR /opt/aerospike-firefly

# Install Firefly
RUN if [[ $RELEASE_BUILD -eq "1" ]] ; \
then mvn install:install-file \
        -Dfile=/opt/aerospike-firefly/aerospike-graph-gremlin/target/aerospike-graph-gremlin-1.1.0.jar \
        -DgroupId=com.aerospike \
        -DartifactId=aerospike-graph-gremlin \
        -Dversion=1.1.0 \
        -Dpackaging=jar \
        -DgeneratePom=true ; \
else mvn install:install-file \
        -Dfile=/opt/aerospike-firefly/aerospike-graph-gremlin/target/aerospike-graph-gremlin-1.1.0-SNAPSHOT.jar \
        -DgroupId=com.aerospike \
        -DartifactId=aerospike-graph-gremlin \
        -Dversion=1.1.0-SNAPSHOT \
        -Dpackaging=jar \
        -DgeneratePom=true ; \
fi

# Move bulk-loader jar to /opt/bulk-loader.
RUN mkdir /opt/bulk-loader &&\
    mv /opt/aerospike-firefly/aerospike-graph-bulk-loader/target/aerospike-graph-bulk-loader-1.1.0-SNAPSHOT.jar /opt/bulk-loader

# Build CLASSPATH before invoking gremlin-server. This is assigned in the gremlin-server script.
# Note bulk-loader also needs to be in the classpath.
RUN curl -L -o /opt/spark.tgz $SPARK_URL &&\
    tar zxvf /opt/spark.tgz -C /opt/ &&\
    mv /opt/spark-$SPARK_VERSION-bin-hadoop3 /opt/spark &&\
    python3 scripts/generate_classpath.py

# Setup gremlin-server. Install firefly in gremlin-server.
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

# Remove additional conflicting logger jars from spark.
RUN rm /opt/spark/jars/slf4j-* && rm /opt/spark/jars/commons-logging*

# Add scripts and conf to container.
ADD conf/docker-default /opt/aerospike-firefly/conf/docker-default
ADD scripts /opt/aerospike-firefly/scripts

# Make gremlin-server-docker.sh runnable and make files in config dir read/write/executable.
RUN chmod +x scripts/gremlin-server-docker.sh
RUN chmod -R 777 $CONF_DIR

# Add user firefly.
RUN useradd -m firefly

# Copy maven repo to firefly user.
RUN chown firefly:firefly -R /opt/spark && chown firefly:firefly -R /opt/bulk-loader

# Make firefly owner of conf dir.
RUN chown firefly:firefly -R /opt/aerospike-firefly/conf/

RUN rm -rf /root/.m2
