import os
import sys

default_aerospike_port = "3000"
default_aerospike_namespace = "test"
default_firefly_data_model = "packed"

if "AEROSPIKE_HOST" not in os.environ:
    err = "Error: Aerospike host cannot be defaulted."
    print(err)
    raise err
aerospike_host = os.environ.get("AEROSPIKE_HOST")
aerospike_port = os.environ.get("AEROSPIKE_PORT", default_aerospike_port)
aerospike_namespace = os.environ.get("AEROSPIKE_NAMESPACE", default_aerospike_namespace)
firefly_data_model = os.environ.get("FIREFLY_DATA_MODEL", default_firefly_data_model)

# Environment variable could be set to empty string.
if aerospike_host == "":
    err = "Error: Aerospike host cannot be defaulted."
    print(err)
    raise err
if aerospike_port == "":
    aerospike_port = default_aerospike_port
if aerospike_namespace == "":
    aerospike_namespace = default_aerospike_namespace
if firefly_data_model == "":
    firefly_data_model = default_firefly_data_model


with open(sys.argv[1]) as f:
    lines = f.readlines()

for i in range(len(lines)):
    lines[i] = lines[i].replace("aerospike_host_to_replace", aerospike_host)
    lines[i] = lines[i].replace("aerospike_port_to_replace", aerospike_port)
    lines[i] = lines[i].replace("aerospike_namespace_to_replace", aerospike_namespace)
    lines[i] = lines[i].replace("firefly_data_model_to_replace", firefly_data_model)

print("Server config:\n")
print(*lines)

with open(sys.argv[1], 'w') as f:
    f.writelines(lines)

