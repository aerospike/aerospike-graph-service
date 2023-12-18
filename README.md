Firefly
-----------
Firefly is an [Apache TinkerPop3®](http://tinkerpop.apache.org) compliant graph database, backed by [Aerospike Enterprise®](https://aerospike.com/products/features-and-editions/).

<img src="https://raw.githubusercontent.com/apache/tinkerpop/master/docs/static/images/tinkerpop-character.png" alt="TinkerPop" width="100"/>
[CI Benchmark results](https://citrusleaf.github.io/firefly/index.html)

Pre-requisites
-----------
To run the latest Firefly, you should have Aerospike Enterprise installed and running.
You can download the latest version of Aerospike Enterprise from [here](https://www.aerospike.com/download/server/).

Running the Latest Firefly Docker Image
-----------
To use the latest Firefly Docker image you must login to `ghcr.io`.

`docker login ghcr.io`

If you are reading this, you should have valid credentials for this. If you do not, talk to Lyndon.

To run the container, use the following command:

`docker run -p8182:8182 -e AEROSPIKE_NAMESPACE="test" -e AEROSPIKE_HOST="<aerospike cluster host IP address>" ghcr.io/citrusleaf/firefly`

Building Custom Firefly with Maven
-----------
`$ mvn -DskipTests clean package`

Building Custom Firefly with Docker
-----------
```
docker build --tag firefly .
docker run -p8182:8182 -e AEROSPIKE_NAMESPACE="test" -e AEROSPIKE_HOST="<aerospike cluster host IP address>" firefly
```

Testing Firefly with Maven
-----------
* Running test cases in local on a Mac needs update to `conf/integration-test-setting-packed.properties`, `conf/integration-test-setting-packed-sindex.properties` and `firefly-spark-bulk-loader/src/test/resources/packed` to change `aerospike.client.host` to `localhost`. 
* Make sure an Aerospike server is running locally. The easiest way is through Docker.

`mvn clean test`

Connecting to a Firefly Server
-----------
If Firefly is running, you can connect to it using any TinkerPop driver.

Below is a sample python script to laod the flights data. You can refer to 
https://kelvinlawrence.net/book/Gremlin-Graph-Guide.html for sample traversals to run on this dataset.
```python
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import IO
import time

def load_flights(g):
    print("Checking if there is already data loaded.")
    if g.V().count().next() > 0:
        print("Dataset already loaded, use g.V().drop().iterate() to remove it.")
        return
    print("Loading flights")
    start = time.time()
    g.io("/opt/air-routes/air-routes-50k.graphml").with_(IO.reader, IO.graphml).read().iterate()
    print("Loading flights took " + str(time.time() - start))

if __name__ == '__main__':
    print("Connecting")
    drc = DriverRemoteConnection('ws://<firefly_ip>:8182/gremlin', 'g')
    g = traversal().withRemote(drc)
    load_flights(g)
    drc.close()
```
