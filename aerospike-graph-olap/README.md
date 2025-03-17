# Aerospike Graph OLAP

## Overview

Aerospike Graph OLAP is a graph analytics engine that provides a high-performance, scalable, and cost-effective solution 
for analyzing large-scale graph data. It is designed to handle large-scale graph data and  to be used in conjunction with
Aerospike Graph Service, which provides a distributed graph database that can store and query large-scale graph data.

## Pre-requisites

- An Aerospike DB loaded with data that you want to analyze.
- A spark cluster to run the OLAP queries. See below for help with this.
- The Aerospike Graph OLAP jar file provided with these instructions.
- A configuration file for Aerospike Graph OLAP that specifies the Aerospike DB connection details.
- Having a vertex label secondary indexes is strongly recommended.
- Configuring Aerospike to handle a lot of secondary indexes is also strongly recommended.
  - Example for setting single-query-threads: `aerolab attach shell -n lyndon-olap-229g -l all -- asinfo -v '"set-config:context=namespace;id=test;single-query-threads=4"'`
  - Example for setting query-threads-limit: `aerolab attach shell -n "$name" -l all -- asinfo -v '"set-config:context=service;query-threads-limit=1024"'`
  - Max secondary indexes that can be run is query-threads-limit / single-query-threads, which is also how many spark executors can work in parallel without errors.

## Setup

The following sections provide instructions on how to set up Aerospike Graph OLAP in GCP and AWS.

### GCP Spark Cluster Setup

To create a spark cluster in GCP, the following can be used from command line, to create a cluster of 20 workers with 8 cores and 64GB memory each.

```bash
dataproc_name="example-spark-cluster-gcp"
region=us-central1
zone=us-central1-a
instance_type=n2d-highmem-8
num_workers=20
num_cores_per_instance=$(echo "$instance_type" | grep -o '[0-9]\+$')
spark_executor_cores=$((num_workers * num_cores_per_instance))
project=example-project
olap_jar="gs://<path>/aerospike-graph-olap-0.0.1.jar"
properties_file_uri="gs://<path>/aerospike-graph-olap.properties"

# This command creates the cluster. If there is a cluster already running, it does not need to be run twice.

gcloud dataproc clusters create "$dataproc_name" \
    --enable-component-gateway \
    --region $region \
    --zone $zone \
    --master-machine-type "$instance_type" \
    --master-boot-disk-type pd-ssd \
    --master-boot-disk-size 500 \
    --num-workers "$num_workers" \
    --worker-machine-type "$instance_type" \
    --worker-boot-disk-type pd-ssd \
    --worker-boot-disk-size 500 \
    --image-version 2.1-debian11 \
    --properties spark:spark.history.fs.gs.outputstream.type=FLUSHABLE_COMPOSITE \
    --project $project

# This command submits the jar file to the spark cluster.
# The configurations set below seems to optimize the performance of the spark cluster well.

gcloud dataproc jobs submit spark \
    --class=com.aerospike.firefly.olap.DistributedGraphComputerMain \
    --jars="$olap_jar" \
    --cluster="$dataproc_name" \
    --region="$region" \
    --properties=\
spark.executor.memory=6g,\
spark.executor.cores=1,\
spark.task.cpus=1,\
spark.executor.instances=$num_cores_per_instance,\
spark.speculation=false,\
spark.dynamicAllocation.enabled=true,\
spark.dynamicAllocation.minExecutors=$num_cores_per_instance,\
spark.dynamicAllocation.maxExecutors=$num_cores_per_instance,\
spark.dynamicAllocation.initialExecutors=$num_cores_per_instance,\
spark.scheduler.minRegisteredResourcesRatio=1.0,\
    -- -c "$properties_file_uri"
```

### AWS Spark Cluster Setup

todo

## Running Queries

Once the jar has been successfully submitted to the spark cluster, you can run OLAP queries using the Gremlin query language.

The one requirement is that the queries must be prefixed with `.withComputer()`. For example, instead of running `g.V().hasLabel("person").count().next()`, 
you would run `g.withComputer().V().hasLabel("person").count().next()`.

To connect to the spark cluster, using a Gremlin Driver, and connect to the master node of the cluster.

### Configuration

The base configuration for Aerospike Graph OLAP is provided in the properties file that is submitted to the spark cluster. However for convenience, any other configs can be overwritten by a query:

The majority of queries run in OLAP are long running queries, and therefore the `evaluationTimeout` should be set.

```
g.withComputer().
    with("evaluationTimeout", 24 * 60 * 60 * 1000). # 1 day
    V(). <etc>
```

In the below example, the amount of pages held in that pagination queue is reduced to lower the memory footprint of the OLAP queries:
```
g.withComputer().
    with("aerospike.graph.pagination.page.queue.size", "2").
    V(). <etc>
```

OLAP has two configs that can only be used in the query itself:
```
g.withComputer().
    with("aerospike.graph.olap.debug.df", "true"). # Enables debugging, this will provide logs in the spark cluster that can help understand what is going on if there are issues.
    with("aerospike.graph.olap.partitions", 160). # Allows you to force the number of partitions to use to be higher or lower than executor count.
    V(). <etc>
```

### Considerations when Running Queries

- Only one query should be run at a time, and the Aerospike cluster being used should ideally not be under stress from other applications.
- The first query run may have worse performance than subsequent queries, due to JIT compiling and other factors with the JVM.
  - Currently, queries that start with edges (`g.withComputer().E()`) do not support secondary indexes and will have worse performance that vertex based queries.
- Queries that can leverage a secondary index to start the query will be much more performant than queries that cannot. For example, if you have a label index, any `g.withComputer().V().hasLabel("")` queries, will be much faster.
- Avoid running OLAP queries on the same Aerospike cluster that is being used for OLTP queries. This is because OLAP queries can be resource-intensive and can impact the performance of OLTP queries.
- Avoid running OLAP queries that return a large amount of data. For example: `g.withComputer().V().hasLabel("person").out().toList()` will return all the vertices connected to the vertices of type "person". This can result in a very large amount of data being returned.
  - Instead, run queries like: `g.withComputer().V().hasLabel("person").groupCount().by(__.out().count())` which will return the count of vertices connected to each vertex of type "person".
  - Or run queries like: `g.withComputer().V().hasLabel("person").out().groupCount().by("name")` which will return the count of vertices connected to each vertex of type "person" grouped by the name of the connected vertices.
- Initial experimentation suggests a ratio of 1 spark executor per 3 Aerospike cores leads to optimal performance. Additional spark resources will likely not improve performance.
  - There are other things to consider such as network performance and disk performance, but this is a good starting point.

