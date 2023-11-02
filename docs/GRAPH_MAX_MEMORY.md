# Setting the max memory of the docker container

The max memory of the docker container and the max memory of the JVM are not the same. Giving the docker container
more memory will not allow the JVM to access more. Gremlin server defaults the max memory of a system to 4 GB unless
it is explicitly overridden.

To explicitly override the max memory of the JVM, you can set the `JAVA_OPTIONS` environment variable in the
docker container to "-Xmx<value_in_mb>m"

For example if we deployed our docker container with:
```
docker run -p 8182:8182 -p9090:9090 -e aerospike.client.host="172.17.0.1" aerospike/aerospike-graph-service
```
and we wanted 32 GB of memory for the JVM, we would run:
```
docker run -p 8182:8182 -p9090:9090 -e JAVA_OPTIONS="-Xmx32768m" -e aerospike.client.host="172.17.0.1" aerospike/aerospike-graph-service
```
