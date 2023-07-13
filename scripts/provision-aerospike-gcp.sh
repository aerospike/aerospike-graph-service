#!/usr/bin/env bash
set -e
instances=${3} # Set number of Aerospike instances in cluster. Default to 3
as_conf=./aerospike.conf
features_file=./features.conf
name=${USER} #set name of cluster to username + optional extra name identifier
instance_type="n2d-highmem-48" # Set instance type for Aerospike nodes in cluster. Default to n2d-highmem-48


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

parsed=$(getopt -a -n provision-aerospike-gcp.sh -o i:c:f:n:o: -- "$@")
eval set -- "$parsed"

while :
do
   case $1 in
        -i) #instance type
#            echo "arg $1 = $2"
            instance_type=$2; shift 2;;
        -c) #cluster count
#            echo "for $1 = $2"
            instances=$2; shift 2;;
        -f) #features file
#            echo "for $1 = $2"
            features_file=$2; shift 2;;
        -n) #cluster name (USER prepended automatically)
#            echo "for $1 = $2"
            name=${USER}$2; shift 2;;
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
=============================
"

echo creating ${name} cluster with ${instances} Aerospikes

# Create Aerospike Cluster but don't start it yet
aerolab cluster create -c ${instances} --instance ${instance_type}  -f $features_file --customconf=$as_conf \
--zone=us-central1-a --disk=pd-ssd:40 --disk=local-ssd --disk=local-ssd --disk=local-ssd --disk=local-ssd \
--disk=local-ssd --disk=local-ssd --disk=local-ssd --disk=local-ssd --name=${name} --start=n;

# Create partitions and blkdiscard
aerolab cluster partition create --filter-type=local --name=${name}

# Update configuration to use devices
aerolab cluster partition conf --namespace=test --configure=device  --filter-type=nvme --filter-partitions=0 --name=${name}

# Start the Aerospikes
aerolab aerospike start --name=${name}

# Add the prometheus exporter to each node of the cluster
aerolab cluster add exporter -n ${name}

# Create the monitoring stack
aerolab client create ams --clusters=${name} --group-name=${name}-ams --zone=us-central1-a --instance=${instance_type} --disk=pd-ssd:40

Hints