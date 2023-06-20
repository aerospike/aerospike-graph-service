# Firefly Spark Bulk Loader

Bulk loading of data using Firefly Bulk Loader is a process that enables the user to load large volumes of graph data
into the database using the Apache Spark distributed computing framework via
the [Gremlin data csv format](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html).

### Requirements

There are two ways of running the bulk loader. The basic method is by invoking the `call` API via a Gremlin Traversal
to an active instance of Firefly. The more advanced method is by running `spark-submit` to a configured Spark cluster.

#### Call API

* A running instance of Firefly
* CSV files containing vertices and edges to be loaded
  in [Gremlin data format](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html)
    - These can live locally or in an AWS S3 bucket

#### Spark Submit

* Hardware with minimum 8GB of RAM
* A running Spark cluster
* Java 11+ installed (for building & running locally)
* CSV files containing vertices and edges to be loaded
  in [Gremlin data format](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html)
    - These can live locally or in an AWS S3 bucket
* A running instance of Aerospike
* `.properties` configuration file for Firefly configured to use that instance

### Vertex and Edge Directories

See configuration below for specifying their location. Local, or cloud (recommended) storage are supported. Currently cloud storage is limited to AWS S3 and Google Cloud Storage.

Within the configured path, both vertices and edges are expected to live in subdirectories that categorize them by type (label). Any amount of CSV files is supported within those subdirectories and the naming under the configured master directory does not need to follow any format. For example:

```
s3://my-bucket/vertices
	-people
		-people_0.csv
		-people_1.csv
		-people_2.csv
    -places
    	-places0.csv
s3://my-bucket/edges
	-livesIn
		-0livesIn.csv
		-1livesIn.csv
		-2livesIn.csv
		-3livesIn.csv
    -visited
    	-visited.csv
```

### Configurations

Running the bulk loader comes with configurable options. These configurations can be accessed and set in one of three ways:

1. Via a `.properties` file. The same configurations found in 2 and 3 as 1 will override the value in the `.properties` file.
2. Call API only: Traversal `.with("configuration_key", configuration_value)` steps appended
3. Command line arguments in the `spark-submit` 

##### Using a `.properties` file

The Firefly Spark Bulk Loader uses the data models within Firefly to accurately load data into Aerospike, so its
configuration needs to be based off an identical `.properties` file to the one that is used to launch Firefly that is
expected to interact with the loaded data. In L2 mode, this configuration file is optional and by default will use the 
running instance's if one is not provided.

Additional Bulk Loader specific configurations should be added to it to create the `.properties` config for it. 

##### Configuration Options

The following configuration options are available:

| Name                                                    | Flag | Optional          | Default            | Description                                                  |
| ------------------------------------------------------- | ---- | ----------------- | ------------------ | ------------------------------------------------------------ |
| aerospike.graphloader.config                            | -c   | Yes if Call API \ | No if Spark Submit | Call API: `.properties` of the instance the Call API is made to. |
| aerospike.graphloader.vertices                          | -vd  | No                | N/A                | Local: Absolute path to directory containing Vertex CSVs. AWS S3: s3:// URI. |
| aerospike.graphloader.edges                             | -ed  | No                | N/A                | Local: Absolute path to directory containing Vertex CSVs. AWS S3: s3:// URI. |
| aerospike.graphloader.keep-provided-edge-id-as-property | -ki  | Yes               | false              | Keep provided ~id value in Edge CSVs as a Property on the Edge. |
| aerospike.graphloader.provided-edge-id-property-name    | -ep  | Yes               | "~providedId"      | Property key/name of provided ID when stored as a Property.  |
| aerospike.graphloader.null-value                        | -nv  | Yes               | "null"             | The String value when found in CSV which is parsed to a literal null. |
| aerospike.graphloader.sampling-percentage               | -sp  | Yes               | 1                  | Percentage of dataset validated to exist properly in the Graph after bulk loading is complete. |
| aerospike.graphloader.spark-log-level                   | -lv  | Yes               | "INFO"             | Spark logger verbosity level. Allowed values: "ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN" |
| aerospike.graphloader.vertex-write-buffer               | -vb  | Yes               | 10000              | Write buffer size for Vertex loading.                        |
| aerospike.graphloader.edge-write-buffer                 | -eb  | Yes               | 10000              | Write buffer size for Edge loading.                          |

#### Cloud Storage Configurations

The Bulk Loader supports using AWS S3 and Google Cloud Storage URIs.

If `aerospike.graphloader.config`, `aerospike.graphloader.vertices`, or `aerospike.graphloader.edges` are configured to cloud URIs the following configurations become relevant. **These configurations cannot be be specified from properties file passed in via `aerospike.graphloader.config`**.

| Name                                 | Flag | Optional  | Description                        |
| ------------------------------------ | ---- | --------- | ---------------------------------- |
| aerospike.graphloader.remote.user    | -u   | Sometimes | See specific cloud service details |
| aerospike.graphloader.remote.passkey | -p   | Sometimes | See specific cloud service details |
| aerospike.graphloader.gcs-email      | -gem | Sometimes | See specific cloud service details |
| aerospike.graphloader.gcs-keyfile    | -gck | Sometimes | See specific cloud service details |

##### AWS S3

For AWS S3, only `aerospike.graphloader.remote.user` and `aerospike.graphloader.remote.passkey` are applicable.

`aerospike.graphloader.remote.user` : aws_access_key_id

`aerospike.graphloader.remote.passkey` : aws_secret_access_key

###### Call API

Both are **not** optional except in the exceptionally niche case where the Docker container running Aerospike Graph Service was preconfigured to have AWS credentials set up in the environment.

###### Spark Submit

Not optional unless the Spark job is going to be run in an environment where AWS credentials are set up.

##### Google Cloud Storage

For Google Cloud Storage, a Service Account must be configured as a prerequisite unless running Spark Submit within Google Cloud. When running the bulk load, you must either specify a local path to the key json file via `aerospike.graphloader.gcs-keyfile`, or specify **all three** of:

`aerospike.graphloader.remote.user` : private_key_id

`aerospike.graphloader.remote.passkey` : private_key

`aerospike.graphloader.gcs-email` : client_email

The values correspond to the key of the same name in the key json file.

###### Call API

Specifying the values directly is recommended as it would be an advanced configuration to allow the key file to be accessible via the Aerospike Graph Service Docker container.

###### Spark Submit

Specifying the path to the key file is recommended over the individual fields. May not be necessary if running from within Google Cloud Services on an account with access to the Aerospike Database also hosted on Google Cloud Services.

#### Example Usage

When using the Call API, simply enter the configuration name as the key and the setting as the value in the `with` step. When using Spark Submit, simply specify the flag and then the value.

##### Call API

The `call` API runs the bulk load on a single Aerospike Graph instance. Because Aerospike Graph is in docker, 
any parameters that are passed must be accessible to the docker image. This is particularly important to consider if 
the `aerospike.graphloader.edges` / `aerospike.graphloader.vertices` are local files. 

In this case you can mount them in the docker container by adding the following to the `docker run` command:

```
-v /path/to/vertices:/path/to/vertices -v /path/to/edges:/path/to/edges
```
A local example:
```
# Docker run with files passed in.  
docker run -p 8182:8182  \
            -v /<local path to root of a directory that contains 'sampledata/vertices' and 'sampledata/edges'>/:/opt/aerospike-firefly/etc/ \
            ghcr.io/citrusleaf/firefly

# Invoke call API with path to files in docker container.
g.call("bulk-load")
    .with("aerospike.graphloader.vertices", "/opt/aerospike-firefly/etc/sampledata/vertices")
    .with("aerospike.graphloader.edges", "/opt/aerospike-firefly/etc/sampledata/edges")
```

Most customers will likely use S3 or GCS, which is the recommended way.
An S3 example is shown below:
```java
g.call("bulk-load").with("aerospike.graphloader.vertices", "s3://myBucket/vertices").with("aerospike.graphloader.edges", "s3://myOtherBucket/edges").with("aerospike.graphloader.remote.user", "myAwsId").with("aerospike.graphloader.remote.passkey", "myAwsSecretKey").iterate();
```

##### Spark Submit

```
spark-submit --conf  spark.driver.memory=17g --conf spark.worker.cleanup.enabled=true --class com.aerospike.firefly.bulkloader.SparkBulkLoader aerospike-graph-bulk-loader-1.0.0-SNAPSHOT.jar -c config.properties -writevertex -dryrun -verifyvertex
```

##### config.properties

```
aerospike.client.host = localhost
aerospike.client.port = 3000
aerospike.client.namespace = test
aerospike.graph.data.model = packed

aerospike.graphloader.edges = src/test/resources/sampledata/edges
aerospike.graphloader.vertices = src/test/resources/sampledata/vertices
```

### Internal-Use Only Configurations

### Command line params

##### Actions

These are the steps to run when bulk loading. The Call API abstracts this away from the user and we handle passing these to the user. When running via Spark Submit we provide the customer the required combination of actions. 

| execution order | param name   |                                       description                                        |
|-----------------|--------------|:----------------------------------------------------------------------------------------:|
| 1               | dryrun       |                                     preflight check                                      |
| 2               | writevertex  |                        write vertices specified by config into db                        |
| 3               | verifyvertex |                      verify post write the sampled vertices dataset                      |
| 4               | writeedge    |        write edges to db, assuming that corresponding vertices are present in db         |
| 5               | verifyedge   |                        verify sampled edges after writing into db                        |

##### Configuration Settings

| Name                                                | Flag | Optional | Default     | Description                                                  |
|-----------------------------------------------------| ---- | -------- | ----------- | ------------------------------------------------------------ |
| aerospike.graphloader.dataframe-caching      | -dc  | Yes      | false       | Dataframe caching state.                                     |
| aerospike.graphloader.dataframe-storage-type | -dt  | Yes      | "disk_only" | Dataframe storage type. Allowed values: "disk_only", "memory_only", "memory_and_disk" |

##### sample commands (for single node L2 with 32 GB memory)

| description           | command                                                      |
| --------------------- | ------------------------------------------------------------ |
| run all vertices task | spark-submit --conf spark.driver.memory=17g --conf spark.worker.cleanup.enabled=true --class com.aerospike.firefly.bulkloader.SparkBulkLoader aerospike-graph-bulk-loader-1.0.0-SNAPSHOT.jar -c c:/config/config.properties -writevertex -dryrun -verifyvertex |
| run all edges task    | spark-submit --conf spark.driver.memory=17g --conf spark.worker.cleanup.enabled=true --class com.aerospike.firefly.bulkloader.SparkBulkLoader aerospike-graph-bulk-loader-1.0.0-SNAPSHOT.jar -c c:/config/config.properties -writeedge -dryrun -verifyedge |

##### sample config file
 ```
aerospike.client.host = 172.31.25.147,172.31.19.243,172.31.30.232
aerospike.client.port = 3000
aerospike.client.namespace = test
aerospike.client.timeout = 70000
aerospike.graph.data.model = packed

aerospike.graphloader.vertices = /home/ubuntu/vertices
aerospike.graphloader.edges = /home/ubuntu/edges
aerospike.graphloader.dataframe-caching = true
aerospike.graphloader.dataframe-storage-type = memory_and_disk  
 ```
#### Setup For Local Spark Submit Cluster

1. [Download the latest version of Apache Spark](https://spark.apache.org/downloads.html) and extract the files
   somewhere on your local
2. Windows:
    - Find the version of Hadoop that matches the Spark download [here](https://github.com/cdarlint/winutils), navigate
      into that version, and download the `winutils.exe` file
    - Put `winutils.exe` into a folder anywhere you want called `bin` and put the `bin` folder into another folder
      called `hadoop`
      Example: `C:\hadoop\bin\winutils.exe`
    - Configure the following environment variables:
        - `SPARK_HOME`: Directory location of the extracted Spark files, e.g. `C:\spark-X.Y.Z-bin-hadoopA.B`
        - `HADOOP_HOME`: Directory location of the `hadoop` directory specified in step 2, e.g. `C:\hadoop`
        - Add `%SPARK_HOME%\bin` and `%HADOOP_HOME%\bin` to the `Path` variable
        - Start Master by running `spark-class org.apache.spark.deploy.master.Master` on a terminal
        - Start a Worker by running `spark-class org.apache.spark.deploy.worker.Worker` on a terminal
3. macOS/Linux:
    - Make sure passwordless ssh is enabled on your local mac (run `ssh localhost` to confirm)
    - Create a `$SPARK_HOME` env variable pointing to `bin` directory in the unzipped `apache-spark` directory in
      your `.bash_profile` script
    - Run `start-all.sh` located at `%SPARK_HOME%/sbin` to start spark master and worker(s)
4. Monitoring spark cluster locally:
    - Starting Master will provide access to a Spark web UI located at `localhost:8080` to track your cluster details
      and job runs. Navigate to `http://localhost:8080` to check if the standalone cluster is up and running
    - The web UI will also provide spark master URL in the format of `spark://192.168.1.69:7077` to used with
      spark-submit command to run your bulk loader

#### Running

* Grab a copy of a `.properties` config to use as a base - one can be found at `firefly/conf/spark-bulk-loader-conf`
* Properly configure it to match your system and desired behaviour
* Build the `firefly-spark-bulk-loader` jar by running `mvn clean install -DskipTests` in the `firefly` root directory
    - This should build a jar located
      at `firefly/firefly-spark-bulk-loader/target/firefly-spark-bulk-loader-X.Y.Z-SNAPSHOT.jar`
    - The jar prefixed with `original` can be ignored
* Submit a spark job locally with the following
  command: `spark-submit --master <SPARK_URL> --conf spark.local.dir=</path/to/local>/work_dir --conf spark.worker.cleanup.enabled=true --conf spark.driver.cores=1 --conf spark.driver.memory=1gb --conf spark.executor.cores=2 --conf spark.executor.instances=2 --conf spark.executor.memoryOverhead=1g --conf spark.executor.memory=2g --conf spark.task.cpus=1 --conf spark.shuffle.service.enable=true --conf spark.sql.shuffle.partitions=100 --conf spark.default.parallelism=100 --conf spark.memory.fraction=0.6 --conf spark.locality.wait=0 --class com.aerospike.firefly.bulkloader.SparkBulkLoader </path/to>/firefly-spark-bulk-loader-X.Y.Z-SNAPSHOT.jar -e local -c <absolute/path/to>/config.properties`
    - Spark uses `/tmp` as default local work/scratch directory. To specify a custom local path on a shared storage,
      make sure to pass the entire path to `spark.local.dir` conf. Spark will auto create `work_dir`
    - The configuration to achieve maximum parallelism in Spark is achieved by allocating the right number of the number
      of cores per executor, memory per executor and number of partitions (parallelism) for task creation as defined in
      the link below when running the bulk-loader in AWS.

#### Running the Bulk Loader in AWS

Follow the
link [here](https://aerospike.atlassian.net/wiki/spaces/PRODUCT/pages/2850324542/Bulk+Loading+Data+using+Firefly+to+Aerospike)
to run the bulk loader in AWS