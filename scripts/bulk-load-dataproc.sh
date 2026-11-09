#!/usr/bin/env bash
# Copyright 2022-2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e
############################################################
# Help                                                     #
############################################################
Help() {

   # Display Help
   echo "
   Syntax: ${0##*/} -j <bulk loader jar> -c <firefly config> [-n <job name>] [-w <number of workers>] [-z <zone>] [-e]

   The Spark cluster is created in <zone> (default us-central1-a) so it sits next to the Aerospike cluster it loads.
   -e runs the job on an existing Spark cluster named <job name> instead of creating one.

   Example: ${0##*/} -j gs://jarbucket/jar/aerospike-graph-bulk-loader-<version>.jar
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
      * Check status of job with 'gcloud  dataproc jobs wait ${name}-job --region=${zone%-*}'
      * When done clean job with 'gcloud dataproc jobs delete ${name}-job --region=${zone%-*}'
      * When done with spark cluster remove it with 'gcloud dataproc clusters delete ${name} --region ${zone%-*}'
"
}
name=spark${USER}${1}
workers=${2:-20}
bulk_jar_uri=XXX
properties_file_uri=XXX
zone=us-central1-a
use_existing_cluster=false

parsed=$(getopt -a -n minimal-spark.sh -o j:c:n:w:z:e -- "$@")
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
        -z) #zone for the spark cluster
            echo "for $1 = $2"
            zone=$2; shift 2;;
        -e) # use an existing spark cluster
            use_existing_cluster=true; shift;;
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

region=${zone%-*}

if [[ "${use_existing_cluster}" != true ]]; then
  bash "$(dirname "${BASH_SOURCE[0]}")/create-dataproc-cluster-gcp.sh" -n ${name} -z ${zone} -w ${workers}
fi

echo "###################################"
echo "running job ${name}"
echo "###################################"

l3_start=`date +%s`

gcloud dataproc jobs submit spark --class=com.aerospike.firefly.bulkloader.SparkBulkLoader --jars=${bulk_jar_uri} --id ${name}-job --cluster=${name} --region=${region} -- -c ${properties_file_uri} -validate_input_data -verify_output_data

echo $((($(date +%s)-$l3_start)/60)) > l3_runtime.txt
