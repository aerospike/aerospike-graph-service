# Firefly Spark Bulk Loader

The Firefly Bulk Loader is a Spark Application that can load large data sets into a Firefly graph database.

### Requirements
* A Spark machine/cluster
* Java installed (for running locally)
* CSV files containing vertices and edges to be loaded in [Gremlin load data format](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html)
  * These can live locally or in an AWS S3 bucket
* A running instance of Aerospike and the `.properties` configuration file for Firefly configured to use that instance

### Configurations

The Firefly Spark Bulk Loader uses the data models within Firefly to accurately load data into Aerospike, so its 
configuration needs to be based off an identical `.properties` file to the one that is used to launch Firefly that is 
expected to interact with the loaded data. Additional Bulk Loader specific configurations should be added to it to 
create the `.properties` config for it. The following configuration options are available:

* `edge_directory` - `String`: The path to where the CSV files containing the edges are stored
  * Each type of edge (i.e. the same label and some properties) **must** be in its own sub-directory within `edge_directory`. Within those sub-directories, the Bulk Loader is able to handle the CSV file split into multiple in cases of extremely large datasets.
* `vertex_directory` - `String`: The path to where the CSV files containing the vertices are stored
  * Each type of vertex (i.e. the same label and some properties) **must** be in its own sub-directory within `vertex_directory`. Within those sub-directories, the Bulk Loader is able to handle the CSV file split into multiple in cases of extremely large datasets.
* `use_provided_edge_id` - `Boolean`: If the `~id` of the edges in the CSV file are whole numeric values, this enables them to be stored with the ID provided. Setting this to true will automatically generate valid edge IDs if they are currently not whole numeric values. Vertex IDs **must** be whole numeric values.
* `keep_provided_edge_id_as_property` - `Boolean`: Store the provided edge ID as a property if not to be used
* `ignore_element_creation_failed` - `Boolean`: If there is a row of data in the provided CSV that is insufficient to create an edge or vertex, `true` will allow the bulk loading job to continue. *Setting this to `true` should be used with caution.*
* `ignore_parse_failed_properties` - `Boolean`: If the value provided for a header with a type specified (see "Property Column Headers" in Gremlin load data format link) cannot be converted to that type, setting this to `true` will allow the bulk loading job to continue.

### Running the Bulk Loader Locally

#### Setup

1. [Download Spark](https://spark.apache.org/downloads.html)
   2. Extract the files somewhere
2. Windows Only: Find the version of Hadoop that matches the Spark download [here](https://github.com/cdarlint/winutils), navigate into that version, and download the `winutils.exe` file
   3. Put `winutils.exe` into a folder anywhere you want called `bin` and put the `bin` folder into another folder called `hadoop`
      4. Example: `C:\hadoop\bin\winutils.exe`
5. Configure the following environment variables:
   6. `SPARK_HOME`: Directory location of the extracted Spark files, e.g. `C:\spark-X.Y.Z-bin-hadoopA.B`
   7. Windows Only: `HADOOP_HOME`: Directory location of the `hadoop` directory specified in step 2, e.g. `C:\hadoop`
   8. Add `%SPARK_HOME%\bin` and `%HADOOP_HOME%\bin` to the `Path` variable
9. Windows:
   10. Start Master by running `spark-class org.apache.spark.deploy.master.Master` on a terminal
   11. Start a Worker by running `spark-class org.apache.spark.deploy.worker.Worker` on a terminal
   12. Starting Master will show you what your Spark URL is in the format of `spark://192.168.1.69:7077`
   13. Starting Master will also give access to a UI located at `localhost:XXXX` to see information such as the Spark URL. The port will be displayed in a message similar to: `Successfully started service 'MasterUI' on port 8080.`
14. macOS/Linux:
    15. (TODO: Unverified) Run `start-all.sh` located at `%SPARK_HOME%\sbin`

#### Running

1. Grab a copy of a `.properties` config to use as a base - one can be found at `firefly/conf/spark-bulk-loader-conf`
2. Properly configure it to match your system and desired behaviour
3. Build the `firefly-spark-bulk-loader` jar by running `mvn clean install -DskipTests` in the `firefly` root directory
   4. This should build a jar located at `firefly\firefly-spark-bulk-loader\target\firefly-spark-bulk-loader-X.Y.Z-SNAPSHOT.jar`
      5. The jar prefixed with `original` can be ignored
3. Fill in and run the following command: `spark-submit --master <SPARK_URL> --conf spark.driver.memory=1g --conf spark.executor.memory=2g --class com.aerospike.firefly.spark.bulkloader.SparkBulkLoader  </path/to>/firefly-spark-bulk-loader-X.Y.Z-SNAPSHOT.jar -e local -c </path/to>/config.properties`

### Running the Bulk Loader on AWS

[TODO](https://aerospike.atlassian.net/wiki/spaces/PRODUCT/pages/2850324542/Bulk+Loading+Data+using+Firefly+to+Aerospike)