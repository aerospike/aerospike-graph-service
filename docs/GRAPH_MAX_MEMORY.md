# Setting the max memory of the docker container

The max memory of the docker container and the max memory of the JVM are not the same. Giving the docker container
more memory will not allow the JVM to access more.

By default, Aerospike Graph uses `-XX:MaxRAMPercentage=80.0`, which tells the JVM to use up to 80% of the
container's available memory for its heap. The JVM automatically detects container memory limits (via cgroup),
so this works correctly in Docker, Fargate, Kubernetes, and other containerized environments.

To explicitly override the max memory of the JVM, you can set the `aerospike.graph-service.heap.max` property
or the `JAVA_OPTIONS` environment variable in the docker container to "-Xmx<value_in_mb>m".

For example, if deploying the docker container with default memory looks like this:
```
docker run -p 8182:8182 -p9090:9090 -e aerospike.client.host="172.17.0.1" ghcr.io/aerospike/firefly
```
then, deploying the same container with 32 GB of memory for the JVM instead would look like:
```
docker run -p 8182:8182 -p9090:9090 -e JAVA_OPTIONS="-Xmx32768m" -e aerospike.client.host="172.17.0.1" ghcr.io/aerospike/firefly
```
