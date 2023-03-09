## Firefly IdentityGraph Generator

* IdentityGenerator is a Runnable class that spawns multiple independent threads/workers each executing their own IdentityGenerator instance.
* Given the nature of the underlying graph structure, it is not necessary for the individual workers to have knowledge/reference to the subgraphs of parallel IdentityGenerators. Such a structure allows for an embarrassingly parallel graph generator.

## Prerequisites

### Running the Graph data generator in AWS
* Instance with ssh security group attached
* AWS Access Key
* AWS Secret Key
* Java 11 installed on AWS EC2

## Running

#### Params to pass to the job
* `e`: Env (local/aws) Default `aws`
* `d`: Full path to directory to store the data. In AWS, provide the subdirectory path after bucket
* `b`: AWS S3 bucket info
* `a`: AWS Access Key
* `s`: AWS Secret Key
* `h`: Number of households to generate data for
* `r`: Number of records to store per file
* `v`: Variance for gaussian distribution. Default = 2. For local debugging/test cases, set it to 0 to see consistent record count

#### Java command to run the jar

AWS:
```
java -jar <path/to>/firefly-generator-X.Y.Z-SNAPSHOT.jar com.aerospike.firefly.generator.identitygraphgenerator.DataGenerator -e aws -b <S3_bucket_name> -d <subdirectory/path/inside/bucket> -a <access_key> -s <secret_key> -h <no_of_households> -r <records_per_file>
```

Local
```
java -jar <path/to>/firefly-generator-X.Y.Z-SNAPSHOT.jar com.aerospike.firefly.generator.identitygraphgenerator.DataGenerator -e local -d <absolute/path/to/data/directory> -h <no_of_households> -r <records_per_file>
```
