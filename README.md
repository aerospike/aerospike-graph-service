Firefly
-----------
Firefly is an [Apache TinkerPop3®](http://tinkerpop.apache.org) compliant graph database, backed by [Aerospike Enterprise®](https://aerospike.com/products/features-and-editions/).

<img src="https://raw.githubusercontent.com/apache/tinkerpop/master/docs/static/images/tinkerpop-character.png" alt="TinkerPop" width="100"/>

Building Firefly
-----------
```
$ mvn -DskipTests clean package  
... 
$ ls firefly-gremlin/target/*.jar  
firefly-gremlin/target/firefly-gremlin-0.1.0-SNAPSHOT-jar-with-dependencies.jar  
firefly-gremlin/target/firefly-gremlin-0.1.0-SNAPSHOT.jar
```

Testing Firefly
----------
The integration tests use the configuration at [firefly-gremlin/src/test/resources/integration-test-settings.properties](https://github.com/citrusleaf/firefly/blob/main/firefly-gremlin/src/test/resources/integration-test-settings.properties). Update these setting to point to your Aerospike instance.

```
aerospike_host = aerospike-ee.server.domain
aerospike_port = 3000
aerospike_namespace = firefly_graph
```

```
$ mvn clean test  
```

![Gremlin](https://raw.githubusercontent.com/apache/tinkerpop/master/docs/static/images/gremlin-gremlin.png)

Installing FireFly in the Gremlin-Console
-----------
copy `firefly-gremlin/src/test/resources/integration-test-settings.properties` to `~/firefly-settings.properties`  
edit `~/firefly-settings.properties` to connect to your Aerospike instance.

```
#install to maven local  
$ mvn -DskipTests clean install  
#allow gremlin console environment to install from maven local  
$ mkdir -p ~/.groovy/ && cp conf/grapeConfig.xml ~/.groovy/
```

![Gremlin-Console](https://raw.githubusercontent.com/apache/tinkerpop/master/docs/static/images/gremlin-console.png)

```
$ ~/software/apache-tinkerpop-gremlin-console-3.6.0/bin/gremlin.sh    
...  
gremlin> :install com.aerospike firefly-gremlin 0.1.0-SNAPSHOT  
==>Loaded: [com.aerospike, firefly-gremlin, 0.1.0-SNAPSHOT] - restart the console to use [aerospike.firefly]  
(exit)  
$ ~/software/apache-tinkerpop-gremlin-console-3.6.0/bin/gremlin.sh  
...  
gremlin> :plugin use aerospike.firefly  
==>aerospike.firefly activated   
gremlin> graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(System.getProperty("user.home")+"/firefly-settings.properties"))    
gremlin> g = graph.traversal()  
==>graphtraversalsource[fireflygraph[172.17.0.1 3000 test], standard]  
```

**NOTE**: It is required that you restart the console after `:install` in order to get the new `ext/firefly` jars on the classpath.

Installing Firefly for Gremlin-Server
-----------

![GremlinServer](https://raw.githubusercontent.com/apache/tinkerpop/master/docs/static/images/gremlin-server.png)

Download [GremlinServer](https://tinkerpop.apache.org/download.html).

The shell command below will load GremlinServer configured to `firefly-gremlin-server.yaml` which references `conf/firefly-gremlin.properties`. The latter is the standard `GRAPH.gremlin` properites file denoting a TinkerPop3 implementation and thus, can be opened using `Graph.open()`.

```
# start GremlinServer
bin/gremlin-server.sh ~/firefly-gremlin-server.yaml
```

Gremlin Traversals
-----------

Gremlin is a concatenative language. There exists a set of approximately 25 'steps' can can be assembled to create complex queries of graph data. The most used 10 steps are presented below for reference.

| step           | example 1                        | example 2                  | description                              |
| -------------- | -------------------------------- | ---------------------------| -----------------------------------------|
| `V`            | `g.V()`                          | `g.V(1,2)`                 |                                          |
| `has`          | `g.V().has('name','gremlin')`    | `g.V().has('age',gt(25))`  |                                          |
| `out`          | `g.V(1).out('knows')`            | `g.V(1).out().out()`       |                                          |
| `in`           |                                  |                            |                                          |
| `count`        | `g.V().count()`                  | `g.V().out().count()`      |                                          |
| `groupCount`   | `g.V().groupCount().by(label)`   | `g.V().out().count()`      |                                          |
| `dedup`        | `g.V().values('age').dedup()`    | `g.V().dedup().by('age')`  |                                          |
| `path`         | `g.V(1).out().out().path()`      |                            |                                          |
| `repeat`       | `g.V(1).repeat(out()).times(2)`  |                            |                                          |
| `where`        |                                  |                            |                                          |
| `select`       |                                  |                            |                                          |
| `as`           |                                  |                            |                                          |

Docker
-----------

To build the docker image for firefly-enabled gremlin-console
```
docker build --build-arg ENTRYPOINT=gremlin.sh -t firefly-console .
```
To use the firefly-gremlin enabled console with 1-touch startup, you may set environment variables to configure your connection,
and pass the console-env-startup script: 
```
$ docker run -t -i -e AEROSPIKE_HOST=172.17.0.1 -e AEROSPIKE_PORT=3000 -e AEROSPIKE_NAMESPACE=test firefly-console -i scripts/console-env-startup.groovy
...
Jun 04, 2022 4:03:56 AM java.util.prefs.FileSystemPreferences$1 run
INFO: Created user preferences directory.

         \,,,/
         (o o)
-----oOOo-(3)-oOOo-----
plugin activated: tinkerpop.server
plugin activated: tinkerpop.utilities
plugin activated: aerospike.firefly
plugin activated: tinkerpop.tinkergraph
gremlin> graph
==>fireflygraph[aerospike://172.17.0.1:3000/test]
gremlin> g
==>graphtraversalsource[fireflygraph[aerospike://172.17.0.1:3000/test], standard]
gremlin> 

```

To build the docker image for firefly-enabled gremlin-server
```
docker build --build-arg ENTRYPOINT=gremlin-server.sh -t firefly-server .
```
