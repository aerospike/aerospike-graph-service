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

# Creates the Dataproc (Spark) cluster the bulk loader runs on. The master and
# every worker use the same machine type.
#
# USAGE: GCP_PROJECT_ID=<project> create-dataproc-cluster-gcp.sh -n <name> -z <zone> [-w <workers>] [-m <machine type>]

set -eo pipefail

name=""
zone=""
workers=4
machine_type=c3d-standard-4

parsed=$(getopt -a -n create-dataproc-cluster-gcp.sh -o n:z:w:m: -- "$@")
eval set -- "$parsed"

while :
do
   case $1 in
        -n) name=$2; shift 2;;
        -z) zone=$2; shift 2;;
        -w) workers=$2; shift 2;;
        -m) machine_type=$2; shift 2;;
        --) shift ; break ;;
        *) echo "Error: Invalid option '$1'"; exit 2;;
   esac
done

if [[ -z "${name}" || -z "${zone}" ]]; then
  echo "USAGE: ${0##*/} -n <name> -z <zone> [-w <workers>] [-m <machine type>]" >&2
  exit 2
fi

gcp_project_id=${GCP_PROJECT_ID:?GCP_PROJECT_ID must be set}
region=${zone%-*}

echo "###################################"
echo "creating spark cluster ${name} with ${workers} ${machine_type} workers in ${zone}"
echo "###################################"

#TODO GRAPH-897: Replace with commented line for JDK 17 bulk loading if GCP images still don't support JDK 17 natively
#gcloud dataproc clusters create ${name} --enable-component-gateway --region ${region} --zone ${zone} --initialization-actions=gs://gha-ci-firefly-bulkloader/scripts/install-jdk-17.sh --properties 'spark-env:JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64,spark:spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' --master-machine-type ${machine_type} --master-boot-disk-type pd-ssd --master-boot-disk-size 500 --num-workers ${workers} --worker-machine-type ${machine_type} --worker-boot-disk-type pd-ssd --worker-boot-disk-size 500 --image-version 2.2-debian12 --properties spark:spark.history.fs.gs.outputstream.type=FLUSHABLE_COMPOSITE --project "${gcp_project_id}"
gcloud dataproc clusters create ${name} --tags=aerolab-client --enable-component-gateway --region ${region} --zone ${zone} --master-machine-type ${machine_type} --master-boot-disk-type pd-ssd --master-boot-disk-size 500 --num-workers ${workers} --worker-machine-type ${machine_type} --worker-boot-disk-type pd-ssd --worker-boot-disk-size 500 --image-version 2.2-debian12 --properties spark:spark.history.fs.gs.outputstream.type=FLUSHABLE_COMPOSITE --project "${gcp_project_id}"
