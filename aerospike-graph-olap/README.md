# Aerospike Graph OLAP

> This module uses the `firefly` codename in class names and a few
> identifiers. The product is **Aerospike Graph Service**; see the
> root [README](../README.md) for why the codename is preserved.

## Overview

Aerospike Graph OLAP is the analytics-side counterpart to Aerospike
Graph Service. It runs Gremlin OLAP traversals (`GraphComputer` jobs —
PageRank, connected components, custom VertexPrograms, large
aggregations) against the same Aerospike namespace that the online
service writes, using Spark as the compute layer.

## Pre-requisites

- An Aerospike DB loaded with data that you want to analyze.
- A spark cluster to run the OLAP queries. See below for help with this.
- The Aerospike Graph OLAP jar file provided with these instructions.
- A configuration file for Aerospike Graph OLAP that specifies the Aerospike DB connection details.
- Having a vertex label secondary indexes is strongly recommended.
- Configuring Aerospike to handle a lot of secondary indexes is also strongly recommended.
  - Example for setting single-query-threads: `aerolab attach shell -n "$name" -l all -- asinfo -v '"set-config:context=namespace;id=test;single-query-threads=4"'`
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
    --image-version 2.2-debian12 \
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

To create a spark cluster in AWS, the following can be used from command line, to create a cluster of 20 workers with 8 cores and 32GB memory each.

The AWS CLI tool must be installed.

```bash
emr_name="olap-spark-cluster-aws"
region=us-east-1
instance_type="m7a.2xlarge"
num_workers=20
num_cores_per_instance=$(aws ec2 describe-instance-types --instance-types "$instance_type" --query "InstanceTypes[0].VCpuInfo.DefaultVCpus" --output text --region $region)
olap_jar="s3://<path>/aerospike-graph-olap-0.0.1.jar"
properties_file_uri="s3://<path>/aerospike-graph-olap.properties"
step_name="olap-step"
log_uri="s3://<path>/logs/"

# Recommend to keep the subnetid (and AWS reigon) same as of Aerospike cluster
# (assuming they're in the AWS as well), to have a hassle free communication between DB and Spark cluster
# If DB is created using aerolab, the subnetid will be printed during cluster create
subnet_id="subnet-04bc1bfb6c6ebc05b"

# Aerolab also prints out the security-group associated with the server nodes.
#   The aerolab log looks like following
#   Using security group ID sg-030a778997ce044eb name AeroLabServer-0eb2d9ae66bac4b47
security_group="sg-028ccc8c880bd48cd"

# Switch from Java 8 to Java 11, the minimum Java version required by Aerospike Graph Service.
CONFIGURATIONS='[{"Classification":"hadoop-env","Configurations":[{"Classification":"export","Configurations":[],"Properties":{"JAVA_HOME":"/usr/lib/jvm/java-11-amazon-corretto.x86_64"}}],"Properties":{}},{"Classification":"spark-env","Configurations":[{"Classification":"export","Configurations":[],"Properties":{"JAVA_HOME":"/usr/lib/jvm/java-11-amazon-corretto.x86_64"}}],"Properties":{}},{"Classification":"spark-defaults","Properties":{"spark.executorEnv.JAVA_HOME":"/usr/lib/jvm/java-11-amazon-corretto.x86_64"}}]'
EMR_RELEASE="emr-7.12.0"

# Create EMR Cluster
echo "Creating EMR Cluster..."

# This command creates the cluster. If there is a cluster already running, it does not need to be run twice.

CLUSTER_ID=$(aws emr create-cluster \
    --name "$emr_name" \
    --release-label "$EMR_RELEASE" \
    --applications Name=Spark \
    --log-uri "$log_uri" \
    --use-default-roles \
    --instance-type "$instance_type" \
    --instance-count "$num_workers" \
    --ec2-attributes SubnetId="$subnet_id",EmrManagedSlaveSecurityGroup="$security_group",EmrManagedMasterSecurityGroup="$security_group" \
    --configurations "$CONFIGURATIONS" \
    --query 'ClusterId' \
    --region "$region" \
    --output text)


# CLUSTER_ID="j-ABC123"
echo "$CLUSTER_ID"

# This command submits the jar file to the spark cluster.
# The configurations set below seems to optimize the performance of the spark cluster well.

# Add Step to run Spark job
aws emr add-steps --cluster-id "$CLUSTER_ID" --steps '[
  {
    "Name": "'"$step_name"'",
    "ActionOnFailure": "CONTINUE",
    "Type": "Spark",
    "Args": [
        "--class", "com.aerospike.firefly.olap.DistributedGraphComputerMain", 
        "--conf", "spark.executor.memory=6g",
        "--conf", "spark.executor.cores=1",
        "--conf", "spark.task.cpus=1",
        "--conf", "spark.executor.instances='"$num_cores_per_instance"'",
        "--conf", "spark.speculation=false",
        "--conf", "spark.dynamicAllocation.enabled=true",
        "--conf", "spark.dynamicAllocation.minExecutors='"$num_cores_per_instance"'",
        "--conf", "spark.dynamicAllocation.maxExecutors='"$num_cores_per_instance"'",
        "--conf", "spark.dynamicAllocation.initialExecutors='"$num_cores_per_instance"'",
        "--conf", "spark.scheduler.minRegisteredResourcesRatio=1.0",
        "'$olap_jar'",
        "-c", "'"$properties_file_uri"'"
      ]
    }
]' \
    --query 'StepIds[0]' \
    --output text \
    --region "$region"
```

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
    with("aerospike.graph.analytics.debug.df", "true"). # Enables debugging, this will provide logs in the spark cluster that can help understand what is going on if there are issues.
    with("aerospike.graph.analytics.partitions", 160). # Allows you to force the number of partitions to use to be higher or lower than executor count.
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

