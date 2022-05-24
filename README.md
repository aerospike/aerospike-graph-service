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
$ mkdir -p  ~/software/apache-tinkerpop-gremlin-console-3.6.0/ext/aerospike-firefly/plugin/ ~/software/apache-tinkerpop-gremlin-console-3.6.0/ext/aerospike-firefly/lib/    
$ cp firefly-gremlin/target/firefly-gremlin-0.0.1-SNAPSHOT.jar ~/software/apache-tinkerpop-gremlin-console-3.6.0/ext/aerospike-firefly/plugin/  
$ cp firefly-gremlin/target/firefly-gremlin-0.0.1-SNAPSHOT-jar-with-dependencies.jar ~/software/apache-tinkerpop-gremlin-console-3.6.0/ext/aerospike-firefly/lib/    
$ echo >> ~/software/apache-tinkerpop-gremlin-console-3.6.0/ext/plugins.txt  
$ echo 'com.aerospike.firefly.jsr223.FireflyGremlinPlugin' >> ~/software/apache-tinkerpop-gremlin-console-3.6.0/ext/plugins.txt    
$ export CLASSPATH=$CLASSPATH:./firefly-gremlin/target/firefly-gremlin-0.0.1-SNAPSHOT-jar-with-dependencies.jar  
$ ~/software/apache-tinkerpop-gremlin-console-3.6.0/bin/gremlin.sh  
...  
plugin activated: aerospike.firefly  
gremlin>  
gremlin> graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(System.getProperty("user.home")+"/firefly-settings.properties"))    
gremlin> 


