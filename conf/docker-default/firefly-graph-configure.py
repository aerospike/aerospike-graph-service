import sys
if len(sys.argv) != 2 and len(sys.argv) != 3:
    print("firefly-graph-configure.py: Expected either both a properties file and an aerospike host ip or just a "
          "properties file. Instead got " + str(sys.argv))
    sys.exit(1)
host = sys.argv[2] if len(sys.argv) == 3 else "172.17.0.1" # Default docker ip address.

print("Config arguments: " + str(sys.argv))

with open(sys.argv[1]) as f:
    lines = f.readlines()

for i in range(len(lines)):
    lines[i] = lines[i].replace("aerospike_host_to_replace", host)

print("Server config:")
print(*lines, sep="\t")

with open(sys.argv[1], 'w') as f:
    f.writelines(lines)

