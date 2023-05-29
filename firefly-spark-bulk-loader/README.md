# Firefly Spark Bulk Loader

Bulk loading of data using Firefly Bulk Loader is a process that enables the user to load large volumes of graph data
into the database using the Apache Spark distributed computing framework via
the [Gremlin data csv format](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html).

### Requirements

* Hardware with minimum 8GB of RAM
* Locally running Spark cluster
* Java 11+ installed (for building & running locally)
* CSV files containing vertices and edges to be loaded
  in [Gremlin data format](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html)
    - These can live locally or in an AWS S3 bucket
* A running instance of Aerospike
* `.properties` configuration file for Firefly configured to use that instance

### Configurations

The Firefly Spark Bulk Loader uses the data models within Firefly to accurately load data into Aerospike, so its
configuration needs to be based off an identical `.properties` file to the one that is used to launch Firefly that is
expected to interact with the loaded data.
Additional Bulk Loader specific configurations should be added to it to create the `.properties` config for it. The
following configuration options are available:

* `edge_directory` - `String`: The path to where the CSV files containing the edges are stored
    - Each type of edge (i.e. the same label and some properties) **must** be in its own sub-directory
      within `edge_directory`.
* `vertex_directory` - `String`: The path to where the CSV files containing the vertices are stored
    - Each type of vertex (i.e. the same label and some properties) **must** also be in its own sub-directory
      within `vertex_directory`.
* The bulk loader loads different edges and vertices from the sub-directories within the parent `edge_directory`
  and `vertex_directory` by scanning each file and applies a `union` transformation to create a bigger `edge`
  and `vertex` dataset
* `keep_provided_edge_id_as_property` - `Boolean`: Store the provided edge ID as a property if not to be used
* `sampling_percentage` - Indicates how much of the input data to be sampled for verifying if the bulk load was
  successful (default is 0.1%).
* `vertex_write_buffer` - (default is 10000) Decides how many vertices should be processed i.e. written to DB before accepting new records in each partition. Once we reach this point, we block until prior tasks are successfully completed.  
* `edge_write_buffer` - (default is 10000) Decides how many edges should be processed i.e. written to DB before accepting new records in each partition. Once we reach this point, we block until prior tasks are successfully completed.  
   
### Command line params

##### params
| execution order | param name   |                                       description                                        |
|-----------------|--------------|:----------------------------------------------------------------------------------------:|
| 1               | dryrun       |                                     preflight check                                      |
| 2               | writevertex  |                        write vertices specified by config into db                        |
| 3               | verifyvertex |                      verify post write the sampled vertices dataset                      |
| 4               | supernode    | extract supernode and set it inside the internal datastructure, which will be used later | 
| 5               | writeedge    |        write edges to db, assuming that corresponding vertices are present in db         |
| 6               | verifyedge   |                        verify sampled edges after writing into db                        |


##### sample commands (for single node L2 with 32 GB memory)
 | description          |                                                                                                                                commnad                                                                                                                                |
 |----------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
 | run all vetices task | spark-submit --conf  spark.driver.memory=17g  --conf spark.worker.cleanup.enabled=true  --class com.aerospike.firefly.bulkloader.SparkBulkLoader firefly-spark-bulk-loader-0.7.0-SNAPSHOT.jar -m local -c config.properties -writevertex  -dryrun -verifyvertex |
 | run all edges task   |    spark-submit --conf  spark.driver.memory=17g  --conf spark.worker.cleanup.enabled=true  --class com.aerospike.firefly.bulkloader.SparkBulkLoader firefly-spark-bulk-loader-0.7.0-SNAPSHOT.jar -m local -c config.properties -writeedge -verifyedge -dryrun     |
 
##### sample config file
 ```
aerospike.client.host = 172.31.25.147,172.31.19.243,172.31.30.232
aerospike.client.port = 3000
aerospike.client.namespace = test
aerospike.client.timeout = 70000
aerospike.graph.data.model = packed

vertex_directory = /home/ubuntu/vertices
edge_directory = /home/ubuntu/edges
enable_dataframe_caching = true
dataframe_storage_type = memory_and_disk
 ```
<br />  

#### Setup

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