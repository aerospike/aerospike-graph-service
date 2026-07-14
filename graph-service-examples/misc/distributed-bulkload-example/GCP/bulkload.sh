#!/bin/bash
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

source set_variables.sh

# Execute the dataproc command creating a cluster
gcloud dataproc clusters create "$dataproc_name" \
    --enable-component-gateway \
    --region $region \
    --zone $zone \
    --master-machine-type "$master_instance_type" \
    --master-boot-disk-type pd-ssd \
    --master-boot-disk-size 75 \
    --num-workers "$num_workers" \
    --worker-machine-type "$instance_type" \
    --worker-boot-disk-type pd-ssd \
    --worker-boot-disk-size 75 \
    --image-version 2.1-debian11 \
    --properties spark:spark.history.fs.gs.outputstream.type=FLUSHABLE_COMPOSITE \
    --project $project

# Bulkload the data
gcloud dataproc jobs submit spark \
    --class=com.aerospike.firefly.bulkloader.SparkBulkLoader \
    --jars="$bulk_jar_uri" \
    --cluster="$dataproc_name" \
    --region="$region" \
    -- -c "$properties_file_uri"