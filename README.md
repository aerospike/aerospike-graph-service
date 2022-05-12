What is Firefly?
-----------
Firefly is an Apache Tinkerpop 3 compliant graph database, backed by Aerospike enterprise

How to build Firefly
-----------
$ mvn -DskipTests clean package  
.....  
$ ls firefly-core/target/*.jar  
firefly-core/target/firefly-core-0.0.1-SNAPSHOT-jar-with-dependencies.jar  
firefly-core/target/firefly-core-0.0.1-SNAPSHOT.jar

How to test Firefly
----------
the integration tests use configuration stored in  
firefly-core/src/test/resources/phaseshift-integration-settings.properties  
update these settings to point to your aerospike instance  

$ mvn clean test  

How to load Firefly in gremlin-console
-----------
copy firefly-core/src/test/resources/phaseshift-integration-settings.properties to ~/firefly-settings.properties  
edit ~/firefly-settings.properties to connect to your aerospike instance

$ CLASSPATH=$(realpath firefly-core/target/firefly-core-0.0.1-SNAPSHOT-jar-with-dependencies.jar) ~/software/apache-tinkerpop-gremlin-console-3.5.2/bin/gremlin.sh  
...  
gremlin>  
gremlin> import com.aerospike.firefly.util.FireflyConfiguration  
gremlin> import com.aerospike.firefly.io.AerospikeConnection  
gremlin> import com.aerospike.firefly.structure.FireflyGraph  
gremlin> c = FireflyConfiguration.loadFromFile(System.getProperty("user.home")+"/firefly-settings.properties")    
gremlin> ac = AerospikeConnection.connect(c.aerospikeHost(), c.aerospikePort(), c.aerospikeNamespace())  
gremlin> graph = FireflyGraph.open(ac, c)  
gremlin> g = graph.traversal()  
gremlin> g.addV("dog").property("color","blue").next()    
==>v[441]  
gremlin> g.V().hasLabel("dog")  
==>v[441]  
