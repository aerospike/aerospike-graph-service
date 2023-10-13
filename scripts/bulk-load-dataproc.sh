#!/usr/bin/env bash
set -e
############################################################
# Help                                                     #
############################################################
Help() {

   # Display Help
   echo "
   Syntax: ${0##*/} -j <bulk loader jar> -c <firefly config> [-n <job name>] [-w <number of workers>]

   Example: ${0##*/} -j gs://jarbucket/jar/aerospike-graph-bulk-loader-2.0.0.jar
   -c gs://configbucket/fireflyconfig/bulk.properties -n check1 -w 10

   Note: This script expects the bulk loader jar and the properties file to be on gs upload these first via something like:

   gcloud storage buckets create gs://mybucket
   gcloud storage cp sparkconfig.properties gs://mybucket/configs/sparkconfig.properties

   Note II: In an effort to not have a bunch of extra options, this just hardcodes the instance types and regions ¯\_(ツ)_/¯

   "
  Hint
}
Hint() {
  echo    "
    Helpful Hints:
      * Check status of job with 'gcloud  dataproc jobs wait ${name}-job --region=us-central1'
      * When done clean job with 'gcloud dataproc jobs delete ${name}-job --region=us-central1'
      * When done with spark cluster remove it with 'gcloud dataproc clusters delete ${name} --region us-central1'
"
}
name=spark${USER}${1}
workers=${2:-20}
bulk_jar_uri=XXX
# bucket=${3:-gs://${USER}}
properties_file_uri=XXX

parsed=$(getopt -a -n minimal-spark.sh -o j:c:n:w: -- "$@")
eval set -- "$parsed"

while :
do
   case $1 in
        -n) #job name
            echo "for $1 = $2"
            name=$2; shift 2;;
        -w) #workers
            echo "for $1 = $2"
            workers=$2; shift 2;;
        -j) #jar for the bulk loader
            echo "for $1 = $2"
            bulk_jar_uri=$2; shift 2;;
        -c) #uri for the bulk loader config file
            echo "for $1 = $2"
            properties_file_uri=$2; shift 2;;
        -h)
            Help
            exit;;
        --) shift ; break ;;
        *) # Invalid option
         echo "Error: Invalid option '$1'"
         Help
         exit;;
   esac
done

if [ ${bulk_jar_uri} = XXX ] || [ ${properties_file_uri} = XXX ]
then
    echo "Missing mandatory options."
    echo
    Help
    exit
fi

echo "###################################"
echo "creating spark cluster ${name}"
echo "###################################"

gcloud dataproc clusters create ${name} --enable-component-gateway --region us-central1 --zone us-central1-a --master-machine-type n2d-standard-4 --master-boot-disk-type pd-ssd --master-boot-disk-size 500 --num-workers ${workers} --worker-machine-type n2-standard-4 --worker-boot-disk-type pd-ssd --worker-boot-disk-size 500 --image-version 2.1-debian11 --properties spark:spark.history.fs.gs.outputstream.type=FLUSHABLE_COMPOSITE --project firefly-aerospike

echo "###################################"
echo "running job ${name}"
echo "###################################"

l3_start=`date +%s`

gcloud dataproc jobs submit spark  --class=com.aerospike.firefly.bulkloader.SparkBulkLoader --jars=${bulk_jar_uri} --id ${name}-job --cluster=${name} --region=us-central1 -- -c ${properties_file_uri} -validate_input_data -verify_output_data

echo $((($(date +%s)-$l3_start)/60)) > l3_runtime.txt
