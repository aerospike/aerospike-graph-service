#!/bin/bash

#This script assumes that user have installed aws cli.
#This means there is a default profile which have permission to create EC2 resources.

CLUSTER_NAME="Aerospike AWS Graph Cluster"
EMR_RELEASE="emr-7.12.0"

#Application logs will be generated here
LOG_URI="s3://l3-load/logs/"
SPARK_JOB_NAME="Aerospike Graph AWS Spark Job"
SPARK_CLASS="com.aerospike.firefly.bulkloader.SparkBulkLoaderMain"
SPARK_JAR="s3://l3-load/jars/aerospike-graph-bulk-loader-2.0.0.jar"

#User may add more params suported by bulkloader
SPARK_ARGS="-c,s3://l3-load/configs/bulk-loader.properties"
AWS_REGION="us-west-1"

#Recommend to keep the subnetid (and AWS reigon) same as of Aerospike cluster
#(assuming they're in the AWS as well), to have a hassle free communication between DB and Spark cluster
# When DB is created using aerolab, it prints its subnetid
SUBNET_ID="subnet-0dd99ce4a766851df"

#Aerolab also prints out the security-group associated with the server nodes. The aerolab log looks like following
#Using security group ID sg-030a778997ce044eb name AeroLabServer-0eb2d9ae66bac4b47
SECURITY_GROUP="sg-030a778997ce044eb"

#Switch from java8 to java 11, the minimum java version needed for Aerospike Firefly Graph.
CONFIGURATIONS='[{"Classification":"hadoop-env","Configurations":[{"Classification":"export","Configurations":[],"Properties":{"JAVA_HOME":"/usr/lib/jvm/java-11-amazon-corretto.x86_64"}}],"Properties":{}},{"Classification":"spark-env","Configurations":[{"Classification":"export","Configurations":[],"Properties":{"JAVA_HOME":"/usr/lib/jvm/java-11-amazon-corretto.x86_64"}}],"Properties":{}},{"Classification":"spark-defaults","Properties":{"spark.executorEnv.JAVA_HOME":"/usr/lib/jvm/java-11-amazon-corretto.x86_64"}}]'

# Create EMR Cluster
echo "Creating EMR Cluster..."

CLUSTER_ID=$(aws emr create-cluster \
    --name "$CLUSTER_NAME" \
    --release-label "$EMR_RELEASE" \
    --applications Name=Spark \
    --log-uri "$LOG_URI" \
    --use-default-roles \
    --instance-type m5.xlarge \
    --instance-count 4 \
    --ec2-attributes SubnetId="$SUBNET_ID",EmrManagedSlaveSecurityGroup="$SECURITY_GROUP",EmrManagedMasterSecurityGroup="$SECURITY_GROUP" \
    --configurations "$CONFIGURATIONS" \
    --query 'ClusterId' \
    --region "$AWS_REGION" \
    --output text)

#clusterid may be used later for operational purposes like deleting
echo "Cluster ID: $CLUSTER_ID"

# Add Step to run Spark job
echo "Adding Spark job step..."
STEP_ID=$(aws emr add-steps --cluster-id "$CLUSTER_ID" \
    --steps Type=Spark,Name="$SPARK_JOB_NAME",ActionOnFailure=CONTINUE,Args=[--class,"$SPARK_CLASS","$SPARK_JAR",$SPARK_ARGS] \
    --query 'StepIds[0]' \
    --output text \
    --region "$AWS_REGION")

echo "Step ID: $STEP_ID"
