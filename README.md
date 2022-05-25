What is Firefly?
-----------
Firefly is an Apache Tinkerpop 3 compliant graph database, backed by Aerospike enterprise

How to build Firefly
-----------
$ mvn -DskipTests clean package  
.....  
$ ls firefly-gremlin/target/*.jar  
firefly-gremlin/target/firefly-gremlin-0.0.1-SNAPSHOT-jar-with-dependencies.jar  
firefly-gremlin/target/firefly-gremlin-0.0.1-SNAPSHOT.jar

How to test Firefly
----------
the integration tests use configuration stored in  
firefly-gremlin/src/test/resources/phaseshift-integration-settings.properties  
update these settings to point to your aerospike instance  

$ mvn clean test  

How to load Firefly in gremlin-console
-----------
copy firefly-gremlin/src/test/resources/phaseshift-integration-settings.properties to ~/firefly-settings.properties  
edit ~/firefly-settings.properties to connect to your aerospike instance  

#install to maven local  
$ mvn -DskipTests clean install  
#allow gremlin console environment to install from maven local  
$ mkdir -p ~/.groovy/ && cp samples/grapeConfig.xml ~/.groovy/  

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


