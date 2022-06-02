Firefly
-----------
Firefly is an [Apache TinkerPop3®](http://tinkerpop.apache.org) compliant graph database, backed by [Aerospike Enterprise®](https://aerospike.com/products/features-and-editions/).

Building Firefly
-----------
```
$ mvn -DskipTests clean package  
... 
$ ls firefly-gremlin/target/*.jar  
firefly-gremlin/target/firefly-gremlin-0.0.1-SNAPSHOT-jar-with-dependencies.jar  
firefly-gremlin/target/firefly-gremlin-0.0.1-SNAPSHOT.jar
```

Testing Firefly
----------
The integration tests use the configuration at [firefly-gremlin/src/test/resources/phaseshift-integration-settings.properties](url). Update these setting to point to your Aerospike instance.

```
aerospike_host = aerospike-dev.phaseshift.internal
aerospike_port = 3000
aerospike_namespace = firefly_graph
```

```
$ mvn clean test  
```

![Gremlin](https://raw.githubusercontent.com/apache/tinkerpop/master/docs/static/images/gremlin-gremlin.png)

Installing FireFly in the Gremlin-Console
-----------
copy `firefly-gremlin/src/test/resources/phaseshift-integration-settings.properties` to `~/firefly-settings.properties`
edit `~/firefly-settings.properties` to connect to your Aerospike instance.

```
#install to maven local  
$ mvn -DskipTests clean install  
#allow gremlin console environment to install from maven local  
$ mkdir -p ~/.groovy/ && cp samples/grapeConfig.xml ~/.groovy/
```

![Gremlin-Console](https://raw.githubusercontent.com/apache/tinkerpop/master/docs/static/images/gremlin-console.png)

```
$ ~/software/apache-tinkerpop-gremlin-console-3.6.0/bin/gremlin.sh    
...  
gremlin> :install com.aerospike firefly-gremlin 0.0.1-SNAPSHOT  
==>Loaded: [com.aerospike, firefly-gremlin, 0.0.1-SNAPSHOT] - restart the console to use [aerospike.firefly]  
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

Gremlin Traversals
-----------

Gremlin is a concatenative language. There exists a set of approximately 25 'steps' can can be assembled to create complex queries of graph data. The most used 10 steps are presented below for reference.

| step           | example 1                      | example 2                  | description                              |
| -------------- | ------------------------------ | ---------------------------| -----------------------------------------|
| `V`            | `g.V()`                        | `g.V(1,2)`                 |                                          |
| `has`          | `g.V().has('name','gremlin')`  | `g.V().has('age',gt(25))`  |                                          |
| `out`          | `g.V(1).out('knows')`          | `g.V(1).out().out()`       |                                          |
| `in`           |                                |                            |                                          |
| `count`        | `g.V().count()`                | `g.V().out().count()`      |                                          |
| `groupCount`   | `g.V().groupCount().by(label)` | `g.V().out().count()`      |                                          |
| `dedup`        | `g.V().values('age').dedup()`  | `g.V().dedup().by('age')`  |                                          |
