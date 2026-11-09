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
# Instance types to try, in order, once instance_type has no capacity in any zone.
# Non "-lssd" types get ssd_count local SSDs attached, so they must accept that count.
fallback_instance_types=""
storage_type=mmd # Storage architecture of primary/secondary indexes and data. Default to mmd
# Zones to try in order until one has capacity. They must all be in us-central1,
# which the Dataproc scripts and CI workflows use as their region.
zones="us-central1-a us-central1-b us-central1-c us-central1-f"
zone_file=./aerospike_zone.txt # Receives the zone the cluster was created in
instance_type_file=./aerospike_instance_type.txt # Receives the instance type the cluster was created with
# Optional aerolab client and Dataproc cluster created in the same zone as the
# Aerospike cluster. A zone is only used if all of them fit in it, and each one
# tries its instance types in order within the zone.
client_name=""
client_instance_types="c3d-standard-8 n2d-standard-8"
client_instance_type_file=./client_instance_type.txt # Receives the instance type the client was created with
dataproc_name=""
dataproc_workers=4
dataproc_machine_types="c3d-standard-4 n2d-standard-4"
dataproc_machine_type_file=./dataproc_machine_type.txt # Receives the machine type the Dataproc cluster was created with
diagnostics_dir=./gcp-diagnostics # Receives node diagnostics when creating or setting up an aerolab cluster or client fails

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

  -F takes a space or comma separated list of instance types to fall back to,
  in order, when the -i instance type has no capacity in any of the zones. The
  instance type that was used is written to $instance_type_file.

  -C <name> also creates an aerolab client (no software) with that name, and
  -D <name> also creates a Dataproc cluster with that name and -W workers
  (default $dataproc_workers), both in the cluster's zone. A zone is skipped when
  any of them has no capacity there. -T and -M take space or comma separated
  lists of instance types to try, in order, for the client (default
  \"$client_instance_types\") and the Dataproc master and workers (default
  \"$dataproc_machine_types\"). The types used are written to
  $client_instance_type_file and $dataproc_machine_type_file.

  When creating or setting up the aerolab nodes fails, diagnostics about them
  are printed and saved under $diagnostics_dir.

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

parsed=$(getopt -a -n provision-aerospike-gcp.sh -o i:F:s:a:c:f:n:o:z:C:T:D:W:M: -- "$@")
eval set -- "$parsed"

while :
do
   case $1 in
        -i) #instance type
            instance_type=$2; shift 2;;
        -F) # fallback instance types
            fallback_instance_types=${2//,/ }; shift 2;;
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
        -C) # client name
            client_name=$2; shift 2;;
        -T) # client instance types
            client_instance_types=${2//,/ }; shift 2;;
        -D) # dataproc cluster name
            dataproc_name=$2; shift 2;;
        -W) # dataproc workers
            dataproc_workers=$2; shift 2;;
        -M) # dataproc machine types
            dataproc_machine_types=${2//,/ }; shift 2;;
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
fallback_instance_types=${fallback_instance_types}
ssd_count=${ssd_count}
storage_type=${storage_type}
zones=${zones}
client_name=${client_name}
client_instance_types=${client_instance_types}
dataproc_name=${dataproc_name}
dataproc_workers=${dataproc_workers}
dataproc_machine_types=${dataproc_machine_types}
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
if [[ -n "${client_name}" ]] && grep -q "^client=${client_name} " <<< "$(aerolab client list -i)"; then
  echo "Error: an aerolab client named ${client_name} already exists. Destroy it before provisioning a new one."
  exit 1
fi
rm -f "${zone_file}" "${instance_type_file}" "${client_instance_type_file}" "${dataproc_machine_type_file}"
gcloud_project_args=(${GCP_PROJECT_ID:+--project=${GCP_PROJECT_ID}})

# Create Aerospike Cluster but don't start it yet.
# "-lssd" instance types (C3/C3D) come with a fixed number of local SSDs
# pre-attached, so the local-ssd disk must not be requested separately as is
# required for machine types (e.g. N2D) that support independently attached
# Local SSDs.
# The default network has GKE subnets in us-central1 as well, and without
# --gcp-subnet aerolab uses whichever subnet of the network it lists first.
create_cluster() {
  local instance=$1
  local zone=$2
  if [[ "$instance" == *-lssd ]] ; then
    aerolab cluster create -c ${instances} --instance ${instance} -v 6.4.0.7 -f $features_file --customconf=$as_conf \
    --zone=${zone} --gcp-subnet=default --disk=pd-ssd:20 --gcp-expire 0 --name=${name} --start=n;
  else
    aerolab cluster create -c ${instances} --instance ${instance} -v 6.4.0.7 -f $features_file --customconf=$as_conf \
    --zone=${zone} --gcp-subnet=default --disk=pd-ssd:20 --disk=local-ssd@${ssd_count} --gcp-expire 0 --name=${name} --start=n;
  fi
}

create_client() {
  local instance=$1
  local zone=$2
  aerolab client create none --zone=${zone} --gcp-subnet=default --disk=pd-ssd:40 --instance ${instance} \
    --gcp-expire 0 -n ${client_name}
}

create_dataproc() {
  local machine_type=$1
  local zone=$2
  bash "${script_dir}/create-dataproc-cluster-gcp.sh" -n "${dataproc_name}" -z "${zone}" -w "${dataproc_workers}" \
    -m "${machine_type}"
}

# A failed create can leave some nodes running, and aerolab numbers the next
# attempt's nodes after them (in any zone), so every node must be gone before
# trying another zone.
remove_cluster() {
  aerolab cluster destroy -f -n "${name}" || true
  bash "${script_dir}/cleanup-aerolab-gcp-instances.sh" "${name}"
}

remove_client() {
  aerolab client destroy -f -n "${client_name}" || true
  bash "${script_dir}/cleanup-aerolab-gcp-instances.sh" "${client_name}"
}

# A Dataproc cluster that failed to create is usually left behind in the ERROR
# state, and its name cannot be reused until it is deleted.
remove_dataproc() {
  local region=${1%-*}
  if gcloud dataproc clusters describe "${dataproc_name}" --region "${region}" "${gcloud_project_args[@]}" \
      > /dev/null 2>&1; then
    gcloud dataproc clusters delete "${dataproc_name}" --region "${region}" "${gcloud_project_args[@]}" --quiet
  fi
}

remove_all() {
  local zone=$1
  remove_cluster
  if [[ -n "${client_name}" ]]; then
    remove_client
  fi
  if [[ -n "${dataproc_name}" ]]; then
    remove_dataproc "${zone}"
  fi
}

port_open() {
  timeout 5 bash -c "</dev/tcp/$1/22" 2> /dev/null
}

# Polls port 22 until it answers or 90 seconds pass, to tell a short SSH outage
# from a node that stays unreachable.
probe_ssh() {
  local ip=$1
  local waited=0
  while true; do
    if port_open "${ip}"; then
      echo "$(date -u +%H:%M:%S) open"
      return
    fi
    echo "$(date -u +%H:%M:%S) closed"
    if (( waited >= 90 )); then
      return
    fi
    sleep 5
    waited=$((waited + 5))
  done
}

# Records what the nodes looked like when SSH to them failed, to tell a guest
# that stopped answering apart from a network path problem: the GCE view of the
# VM, its serial console, port 22 from this machine and from a peer node over
# the internal network, and the boot journal of nodes that still answer.
# aerolab and gcloud read stdin, so they are kept off the loops' input.
collect_diagnostics() {
  local aerolab_name=$1
  local attach=$2
  local dir=$3
  local instances instance zone public_ip internal_ip node
  local peer=""
  mkdir -p "${dir}"
  echo "===== Diagnostics for ${aerolab_name}, saved under ${dir} ====="
  instances=$(gcloud compute instances list "${gcloud_project_args[@]}" \
    --filter="name~'^aerolab4-${aerolab_name}-[0-9]+\$'" \
    --format="value(name,zone.basename(),networkInterfaces[0].accessConfigs[0].natIP,networkInterfaces[0].networkIP)" \
    < /dev/null)
  echo "${instances}" | tee "${dir}/instances.txt"
  while read -r instance zone public_ip internal_ip; do
    if [[ -n "${public_ip}" ]] && port_open "${public_ip}"; then
      peer=${instance##*-}
      break
    fi
  done <<< "${instances}"
  while read -r instance zone public_ip internal_ip; do
    [[ -n "${instance}" ]] || continue
    node=${instance##*-}
    echo "----- ${instance} (${zone}, public ${public_ip}, internal ${internal_ip})"
    gcloud compute instances describe "${instance}" --zone="${zone}" "${gcloud_project_args[@]}" \
      --format="yaml(status,creationTimestamp,lastStartTimestamp,lastStopTimestamp)" < /dev/null \
      | tee "${dir}/${instance}-instance.yaml"
    gcloud compute instances get-serial-port-output "${instance}" --zone="${zone}" "${gcloud_project_args[@]}" \
      < /dev/null > "${dir}/${instance}-serial.log" 2>&1
    echo "Serial console lines about SSH, networking and the guest agent (last 40):"
    grep -Eai 'ssh|network|netplan|dhcp|guest.agent|cloud-init|reboot|link (is )?(up|down)' \
      "${dir}/${instance}-serial.log" | tail -n 40
    echo "Port 22 from this machine:"
    probe_ssh "${public_ip}" | tee "${dir}/${instance}-ssh-from-here.txt"
    if [[ -n "${peer}" && "${peer}" != "${node}" ]]; then
      echo "Port 22 from node ${peer} over the internal network:"
      timeout 60 aerolab attach "${attach}" -n "${aerolab_name}" -l "${peer}" -- \
        bash -c "timeout 5 bash -c '</dev/tcp/${internal_ip}/22' && echo open || echo closed" < /dev/null 2>&1 \
        | tee "${dir}/${instance}-ssh-from-node-${peer}.txt"
    fi
    if port_open "${public_ip}"; then
      timeout 120 aerolab attach "${attach}" -n "${aerolab_name}" -l "${node}" -- \
        sudo journalctl -b --no-pager -o short-precise < /dev/null > "${dir}/${instance}-journal.log" 2>&1
      echo "Boot journal lines about SSH, networking and the guest agent (last 40):"
      grep -Eai 'ssh|systemd-networkd|netplan|dhcp|guest.agent|cloud-init|resolved|link (is )?(up|down)' \
        "${dir}/${instance}-journal.log" | tail -n 40
    else
      echo "Port 22 is not answering, so the boot journal cannot be read"
    fi
  done <<< "${instances}"
}

# Best effort: diagnostics must never change how the script exits.
dump_diagnostics() {
  local kind=$1
  local aerolab_name attach
  case "${kind}" in
    cluster) aerolab_name=${name}; attach=shell;;
    client) aerolab_name=${client_name}; attach=client;;
    *) return 0;;
  esac
  ( set +e; collect_diagnostics "${aerolab_name}" "${attach}" \
    "${diagnostics_dir}/${aerolab_name}-$(date -u +%Y%m%dT%H%M%SZ)" ) || true
}

# Tries the given types in order with create_<kind> in one zone, removing what
# each failed attempt left behind, and sets created_type to the one that worked.
# Returns 1 when none of them had capacity. Any other error would recur in
# every zone, so it removes everything and exits. Callers test its status,
# which turns off set -e inside it.
create_in_zone() {
  local kind=$1
  local zone=$2
  shift 2
  local type
  created_type=""
  for type in "$@"; do
    echo "Attempting to create ${kind} with ${type} in ${zone}"
    if "create_${kind}" "${type}" "${zone}" 2>&1 | tee "${create_log}"; then
      created_type=${type}
      return 0
    fi
    if ! grep -Eq "${retryable_zone_errors}" "${create_log}"; then
      echo "Error: creating ${kind} failed for a reason other than zone capacity, not trying other zones"
      dump_diagnostics "${kind}"
      remove_all "${zone}"
      exit 1
    fi
    echo "Creating ${kind} with ${type} in ${zone} failed for lack of capacity, removing anything it left behind"
    if ! "remove_${kind}" "${zone}"; then
      echo "Error: could not remove what the failed ${kind} attempt left behind"
      exit 1
    fi
  done
  return 1
}

created_zone=""
created_instance_type=""
created_client_instance_type=""
created_dataproc_machine_type=""
provisioned=false
create_log=$(mktemp)
on_exit() {
  local status=$?
  rm -f "${create_log}"
  if [[ ${status} -ne 0 && "${provisioned}" == true ]]; then
    echo "Error: setting up ${name} failed"
    dump_diagnostics cluster
  fi
}
trap on_exit EXIT
candidate_instance_types=(${instance_type} ${fallback_instance_types})
for instance in "${candidate_instance_types[@]}"; do
  for zone in ${zones}; do
    if ! create_in_zone cluster "${zone}" "${instance}"; then
      continue
    fi
    if [[ -n "${client_name}" ]]; then
      if ! create_in_zone client "${zone}" ${client_instance_types}; then
        echo "No client instance type (${client_instance_types}) had capacity in ${zone}, removing ${name}"
        remove_cluster
        continue
      fi
      created_client_instance_type=${created_type}
    fi
    if [[ -n "${dataproc_name}" ]]; then
      if ! create_in_zone dataproc "${zone}" ${dataproc_machine_types}; then
        echo "No Dataproc machine type (${dataproc_machine_types}) had capacity in ${zone}, removing ${name} and its client"
        remove_all "${zone}"
        continue
      fi
      created_dataproc_machine_type=${created_type}
    fi
    created_zone=${zone}
    created_instance_type=${instance}
    break 2
  done
  echo "None of the zones (${zones}) had capacity for ${instances} ${instance} instances and everything created alongside them"
done

if [[ -z "${created_zone}" ]]; then
  echo "Error: none of the instance types (${candidate_instance_types[*]}) had capacity in any of the zones (${zones})"
  exit 1
fi
provisioned=true

node_zones=$(aerolab cluster list -j | jq -r --arg name "${name}" '.[] | select(.ClusterName == $name) | .Zone')
if [[ $(grep -c . <<< "${node_zones}") -ne ${instances} ]] || grep -qvxF "${created_zone}" <<< "${node_zones}"; then
  echo "Error: expected ${instances} ${name} nodes in ${created_zone}, found nodes in:"
  echo "${node_zones}"
  exit 1
fi
echo "${created_zone}" > "${zone_file}"
echo "${created_instance_type}" > "${instance_type_file}"
echo "Created ${name} with ${created_instance_type} in ${created_zone}"
if [[ -n "${client_name}" ]]; then
  echo "${created_client_instance_type}" > "${client_instance_type_file}"
  echo "Created client ${client_name} with ${created_client_instance_type} in ${created_zone}"
fi
if [[ -n "${dataproc_name}" ]]; then
  echo "${created_dataproc_machine_type}" > "${dataproc_machine_type_file}"
  echo "Created Dataproc cluster ${dataproc_name} with ${created_dataproc_machine_type} in ${created_zone}"
fi



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
