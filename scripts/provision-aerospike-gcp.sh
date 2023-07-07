#!/usr/bin/env bash
instances=${1:-3} # Set number of Aerospike instances in cluster. Default to 3
as_conf=${2:-./aerospike.conf}
features_file=${3:-./features.conf}
name=${USER}${2} #set name of cluster to username + optional extra name identifier
instance_type=${instance_type:-"n2d-highmem-48"} # Set instance type for Aerospike nodes in cluster. Default to n2d-highmem-48


Help()
{
  echo "
  Script to provision an aerospike cluster on GCP
  After running this script you will have a working Aerospike cluster
  and a monitoring stack.

  USAGE: ${0##*/} [] [number of instances] [name] [instance type]

  If no arguments are provided will be effectively: '${0##*/} $instances $USER $instance_type'

  Helpful Hints:
  Check your new clusters with 'aerolab cluster list'. Add a '| grep $USER[whatever name you used]' to see just this deployment.
  SSH to your cluster with 'aerolab attach shell --node=1 --name=<your cluster name>'

  Check your monitoring dashboard hosts with 'aerolab clients list'. Find the external IP of your client and access it at http://xxx.xxx.xxx.xxx:3000. The default username and password is 'admin:admin'

  Find all of this exciting stuff and more over at https://github.com/aerospike/aerolab/
  "
}

if [ "$1" = "-h" ] || [ "$1" = "help" ]
then
  Help
  exit
fi


echo creating "${name}" cluster with "${instances}" Aerospikes
# Create Aerospike Cluster
aerolab cluster create -c "${instances}" --instance "${instance_type}"  -f features.conf --customconf=aerospike.conf --zone us-central1-a --disk=pd-ssd:40 --disk=local-ssd --disk=local-ssd --disk=local-ssd --disk=local-ssd --disk=local-ssd --disk=local-ssd --disk=local-ssd --disk=local-ssd --name="${name}" --start=n;

# Create partitions and blkdiscard
aerolab cluster partition create --filter-type=local --name="${name}"

# Update configuration to use devices
aerolab cluster partition conf --namespace=test --configure=device  --filter-type=nvme --filter-partitions=0 --name="${name}"

# Start the Aerospikes
aerolab aerospike start --name="${name}"

# Add the prometheus exporter to each node of the cluster
aerolab cluster add exporter -n "${name}"

# Create the monitoring stack
aerolab client create ams --clusters="${name}" --group-name="${name}"-ams --zone=us-central1-a --instance="${instance_type}" --disk=pd-ssd:40