#!/usr/bin/env bash
set -e
instances=3 # Set number of Aerospike instances in cluster. Default to 3
as_conf=./aerospike.conf
features_file=./features.conf
name=${USER} #set name of cluster to username + optional extra name identifier
instance_type="n2d-standard-4" # Set instance type for Aerospike nodes in cluster. Default to n2d-standard-4
ssd_count=1 # Amount of local ssd to attach to the instances. Each is 375 GiB


Help()
{
  echo "
  Script to provision an aerospike cluster on GCP
  After running this script you will have a working Aerospike cluster
  and a monitoring stack.

  USAGE: ${0##*/} [] [number of instances] [name] [instance type]

  If no arguments are provided will be effectively:
    '${0##*/} -c $instances -n $name -i $instance_type -f $features_file -o $as_conf'

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

parsed=$(getopt -a -n provision-aerospike-gcp.sh -o i:s:c:f:n:o: -- "$@")
eval set -- "$parsed"

while :
do
   case $1 in
        -i) #instance type
            instance_type=$2; shift 2;;
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
=============================
"

echo creating ${name} cluster with ${instances} Aerospikes

# Create Aerospike Cluster but don't start it yet
aerolab cluster create -c ${instances} --instance ${instance_type} -v 6.2.0.7 -f $features_file --customconf=$as_conf \
--zone=us-central1-a --disk=pd-ssd:20 --disk=local-ssd@${ssd_count} --name=${name} --start=n;

# Create partitions
aerolab cluster partition create --name=${name} --filter-type=nvme -p 24,24,24,24

# Update configuration to use devices
aerolab cluster partition conf --name=${name} --namespace=test --filter-type=nvme --filter-partitions=1,2,3,4 --configure=device

# Update configuration to use 80% of available instance memory
aerolab conf namespace-memory --name=${name} --namespace=test --mem-pct=80

# Start the Aerospikes
aerolab aerospike start --name=${name}

# Add the prometheus exporter to each node of the cluster
#aerolab cluster add exporter -n ${name}

# Create the monitoring stack
#aerolab client create ams --clusters=${name} --group-name=${name}-ams --zone=us-central1-a --instance=${instance_type} --disk=pd-ssd:20

Hints