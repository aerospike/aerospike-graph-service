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

set -eo pipefail
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
instances=3 # Set number of Aerospike instances in cluster. Default to 3
as_conf=./aerospike.conf
features_file=./features.conf
name=${USER} #set name of cluster to username + optional extra name identifier
instance_type="c3-standard-4-lssd" # Set instance type for Aerospike nodes in cluster. Default to c3-standard-4-lssd
ssd_count=1 # Amount of local ssd to attach to the instances. Each is 375 GiB
storage_type=mmd # Storage architecture of primary/secondary indexes and data. Default to mmd
# Zones to try in order until one has capacity. They must all be in us-central1,
# which the Dataproc scripts and CI workflows use as their region.
zones="us-central1-a us-central1-b us-central1-c us-central1-f"
zone_file=./aerospike_zone.txt # Receives the zone the cluster was created in

# GCE errors meaning the zone cannot host the request right now. Any other
# failure would repeat in every zone, so it is not retried.
retryable_zone_errors='ZONE_RESOURCE_POOL_EXHAUSTED|stockout|does not have enough resources available|does not exist in zone'


Help()
{
  echo "
  Script to provision an aerospike cluster on GCP
  After running this script you will have a working Aerospike cluster
  and a monitoring stack.

  USAGE: ${0##*/} [] [number of instances] [name] [instance type]

  If no arguments are provided will be effectively:
    '${0##*/} -c $instances -n $name -i $instance_type -f $features_file -o $as_conf -z \"$zones\"'

  -z takes a space or comma separated list of zones. They are tried in order,
  falling back to the next one when GCP has no capacity, and the zone that was
  used is written to $zone_file.

  "
  Hints
}

Hints() {
  echo "
    Helpful Hints:
    * Check your new clusters with 'aerolab cluster list'. Add a '| grep ${USER}<whatever name you used>' to see
      just this deployment.
    * SSH to your cluster with 'aerolab attach shell --node=1 --name=<your cluster name>'
    * Check your monitoring dashboard hosts with 'aerolab clients list'. Find the external IP
      of your client and access it at http://xxx.xxx.xxx.xxx:3000. The default username and password is 'admin:admin'
    * Find all of this exciting stuff and more over at https://github.com/aerospike/aerolab/
  "
}

parsed=$(getopt -a -n provision-aerospike-gcp.sh -o i:s:a:c:f:n:o:z: -- "$@")
eval set -- "$parsed"

while :
do
   case $1 in
        -i) #instance type
            instance_type=$2; shift 2;;
        -a) #storage type
            storage_type=$2; shift 2;;
        -s) #ssd count
            ssd_count=$2; shift 2;;
        -c) #cluster count
            instances=$2; shift 2;;
        -f) #features file
            features_file=$2; shift 2;;
        -n) #cluster name
            name=$2; shift 2;;
        -o) # aerospike config
            as_conf=$2; shift 2;;
        -z) # zones to try
            zones=${2//,/ }; shift 2;;
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

echo "
=============================
beginning to provision with:
instances=$instances
as_conf=$as_conf
features_file=$features_file
name=$name
instance_type=${instance_type}
ssd_count=${ssd_count}
storage_type=${storage_type}
zones=${zones}
=============================
"

echo creating ${name} cluster with ${instances} Aerospikes

# Anything found under this name later on must then have come from this run,
# so it is safe to delete between zone attempts.
existing_clusters=$(aerolab cluster list -i)
if grep -q "^cluster=${name} " <<< "${existing_clusters}"; then
  echo "Error: an aerolab cluster named ${name} already exists. Destroy it before provisioning a new one."
  exit 1
fi
rm -f "${zone_file}"

# Create Aerospike Cluster but don't start it yet.
# "-lssd" instance types (C3/C3D) come with a fixed number of local SSDs
# pre-attached, so the local-ssd disk must not be requested separately as is
# required for machine types (e.g. N2D) that support independently attached
# Local SSDs.
# The default network has GKE subnets in us-central1 as well, and without
# --gcp-subnet aerolab uses whichever subnet of the network it lists first.
create_cluster() {
  local zone=$1
  if [[ "$instance_type" == *-lssd ]] ; then
    aerolab cluster create -c ${instances} --instance ${instance_type} -v 6.4.0.7 -f $features_file --customconf=$as_conf \
    --zone=${zone} --gcp-subnet=default --disk=pd-ssd:20 --gcp-expire 0 --name=${name} --start=n;
  else
    aerolab cluster create -c ${instances} --instance ${instance_type} -v 6.4.0.7 -f $features_file --customconf=$as_conf \
    --zone=${zone} --gcp-subnet=default --disk=pd-ssd:20 --disk=local-ssd@${ssd_count} --gcp-expire 0 --name=${name} --start=n;
  fi
}

# A failed create can leave some nodes running, and aerolab numbers the next
# attempt's nodes after them (in any zone), so every node must be gone before
# trying another zone.
remove_partial_cluster() {
  aerolab cluster destroy -f -n "${name}" || true
  bash "${script_dir}/cleanup-aerolab-gcp-instances.sh" "${name}"
}

created_zone=""
create_log=$(mktemp)
trap 'rm -f "${create_log}"' EXIT
for zone in ${zones}; do
  echo "Attempting to create ${name} in ${zone}"
  if create_cluster "${zone}" 2>&1 | tee "${create_log}"; then
    created_zone=${zone}
    break
  fi
  echo "Creating ${name} in ${zone} failed, removing any nodes it left behind"
  remove_partial_cluster
  if ! grep -Eq "${retryable_zone_errors}" "${create_log}"; then
    echo "Error: creating ${name} failed for a reason other than zone capacity, not trying other zones"
    exit 1
  fi
done

if [[ -z "${created_zone}" ]]; then
  echo "Error: none of the zones (${zones}) had capacity for ${instances} ${instance_type} instances"
  exit 1
fi

node_zones=$(aerolab cluster list -j | jq -r --arg name "${name}" '.[] | select(.ClusterName == $name) | .Zone')
if [[ $(grep -c . <<< "${node_zones}") -ne ${instances} ]] || grep -qvxF "${created_zone}" <<< "${node_zones}"; then
  echo "Error: expected ${instances} ${name} nodes in ${created_zone}, found nodes in:"
  echo "${node_zones}"
  exit 1
fi
echo "${created_zone}" > "${zone_file}"
echo "Created ${name} in ${created_zone}"



if [[ "$storage_type" = "dmd" ]] ; then
  echo "Setting up Aerospike with PI on disk, SI on memory, data on disk"
  # Create partitions
  aerolab cluster partition create --name=${name} --filter-type=nvme -p 12,28,28,28
  # Create a filesystem on partition 1
  aerolab cluster partition mkfs --name=${name} --filter-type=nvme --filter-partitions=1 --fs-type=xfs --mount-options=noatime
  # Update configuration to use all-flash for 1 partition
  aerolab cluster partition conf --name=${name} --namespace=test --filter-type=nvme --filter-partitions=1 --configure=pi-flash
  # Update configuration to use devices for 3 partitions
  aerolab cluster partition conf --name=${name} --namespace=test --filter-type=nvme --filter-partitions=2,3,4 --configure=device
  # Update configuration sprigs for PI on disk
  aerolab conf adjust --name=${name} set "namespace test.partition-tree-sprigs" 16384
elif [[ "$storage_type" = "ddd" ]] ; then
  echo "Setting up Aerospike with PI on disk, SI on disk, data on disk"
  # Create partitions
  aerolab cluster partition create --name=${name} --filter-type=nvme -p 12,6,39,39
  # Create a filesystem on partitions 1-2
  aerolab cluster partition mkfs --name=${name} --filter-type=nvme --filter-partitions=1,2 --fs-type=xfs --mount-options=noatime
  # Update configuration to use all-flash for 1,2 partitions for pi and si
  aerolab cluster partition conf --name=${name} --namespace=test --filter-type=nvme --filter-partitions=1 --configure=pi-flash
  aerolab cluster partition conf --name=${name} --namespace=test --filter-type=nvme --filter-partitions=2 --configure=si-flash
  # Update configuration to use devices for 2 partitions
  aerolab cluster partition conf --name=${name} --namespace=test --filter-type=nvme --filter-partitions=3,4 --configure=device
  # Update configuration sprigs for PI on disk
  aerolab conf adjust --name=${name} set "namespace test.partition-tree-sprigs" 16384
else
  echo "Setting up Aerospike with PI on memory, SI on memory, data on disk"
  # Create partitions
  aerolab cluster partition create --name=${name} --filter-type=nvme -p 24,24,24,24
  # Update configuration to use devices
  aerolab cluster partition conf --name=${name} --namespace=test --filter-type=nvme --filter-partitions=1,2,3,4 --configure=device
fi

# Update configuration to use 80% of available instance memory
aerolab conf namespace-memory --name=${name} --namespace=test --mem-pct=80

# Start the Aerospikes
aerolab aerospike start --name=${name}

# Add the prometheus exporter to each node of the cluster
#aerolab cluster add exporter -n ${name}

# Create the monitoring stack
#aerolab client create ams --clusters=${name} --group-name=${name}-ams --zone=${created_zone} --instance=${instance_type} --disk=pd-ssd:20

Hints
