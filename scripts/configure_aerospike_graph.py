import os, sys

def main(input_properties_file, default_yaml_file, output_yaml_file, output_properties_file, output_java_options_file):
    valid_properties = []
    valid_yaml = []
    invalid = []
    java_options_max_heap = None
    java_options_min_heap = None

    keys = []
    try:
        # May not be provided so try catch this block.
        with open(input_properties_file) as c:
            print("Reading properties file: " + input_properties_file)
            lines = [line.rstrip() for line in c]
            for line in lines:
                if c == "":
                    continue

                if not "=" in line:
                    invalid.append(line)
                keys.append(line.split("=")[0])
                if line.startswith("aerospike.graph-service.max-heap"):
                    java_options_max_heap = line
                elif line.startswith("aerospike.graph-service.min-heap"):
                    java_options_min_heap = line
                elif line.startswith("aerospike.graph-service"):
                    valid_yaml.append(line)
                elif line.startswith("aerospike"):
                    valid_properties.append(line)
                elif not line.startswith("gremlin.graph"):
                    invalid.append(line)
    except:
        pass

    keys_in_both_properties_and_environment = []
    for key, value in os.environ.items():
        if key in keys:
            keys_in_both_properties_and_environment.append(key)
        if key.startswith("aerospike.graph-service.max-heap"):
            java_options_max_heap = f"{key}={value}"
        elif key.startswith("aerospike.graph-service.min-heap"):
            java_options_min_heap = f"{key}={value}"
        elif key.startswith("aerospike.graph-service"):
            valid_yaml.append(f"{key}={value}")
        elif key.startswith("aerospike"):
            valid_properties.append(f"{key}={value}")

    if len(keys_in_both_properties_and_environment) > 0:
        raise Exception("Error configuring Aerospike Graph Service.\n\tThe following keys were found in both the properties file and the environment: " + \
                        str(keys_in_both_properties_and_environment) + \
                        ". Please remove them from either the properties file or the environment.")

    if len(invalid) > 0:
        raise Exception("Error configuring Aerospike Graph Service.\n\tInvalid properties found: " + str(invalid) + ". Properties must start with 'aerospike' and " + \
                    "be in the format 'aerospike.key=value'")

    generate_yaml(valid_yaml, default_yaml_file, output_yaml_file, output_properties_file)
    generate_properties(valid_properties, output_properties_file)
    generate_java_options(output_java_options_file, java_options_max_heap, java_options_min_heap)


def generate_yaml(yaml_properties, default_yaml_file, output_yaml_file, output_properties_file):
    rewritten_lines = []

    # Read yaml lines.
    with open(default_yaml_file) as yaml:
        lines = [line.rstrip() for line in yaml]

    for property in yaml_properties:
        key = property.split("=")[0]
        value = property.split("=")[1]
        key = key.replace("aerospike.graph-service.", "")
        if key == "serializers" or key == "processors" or key == "graphs":
            # These need to be injected in a weird way and it's unlikely there is a good reason to do anything with these.
            # If a customer has a good reason, we will add support for this later.
            # Graphs requires coordinating the properties file and the yaml so should not be overwritten.
            raise Exception("Error configuring Aerospike Graph Service.\n\t'serializers', 'processors', and 'graphs' " + \
                    "of gremlin-server config cannot be overwritten by properties file, contact support if you need " + \
                    "to override these configurations.")
        lines = [i for i in lines if not i.startswith(key)]
        rewritten_lines.append(f"{key}: {value}")

    # Pop serializers in here since we can't flatten them.
    rewritten_lines.append(
"""graphs: { graph: """ + output_properties_file + """}
serializers:
  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3, config: { ioRegistries: [org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3d0] }}          # application/json
  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1 }                                                                                                           # application/vnd.graphbinary-v1.0
  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1, config: { serializeResultToString: true }}                                                                 # application/vnd.graphbinary-v1.0-stringd
processors:
  - { className: org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor, config: { sessionTimeout: 28800000 }}
  - { className: org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor, config: { cacheExpirationTime: 600000, cacheMaxSize: 1000 }}
""")

    lines = lines + rewritten_lines

    with open(output_yaml_file, "w") as yaml:
        for line in lines:
            yaml.write(line + "\n")

    with open(output_yaml_file, "r") as prop:
        print("Generated yaml file: " + output_yaml_file + " - " + prop.read())

def generate_properties(properties, output_properties_file):
    with open(output_properties_file, "w") as prop:
        if "gremlin.graph=com.aerospike.firefly.structure.FireflyGraph" not in properties:
            prop.write("gremlin.graph=com.aerospike.firefly.structure.FireflyGraph\n")
        for property in properties:
            prop.write(property + "\n")

def generate_java_options(java_options_file_path, max_heap, min_heap):
    java_options = ""

    # We are deprecating JAVA_OPTIONS in favor of using our notation. Users don't need to know we are using Java.
    if max_heap is not None:
        print("aerospike.graph-service.max-heap was set to " + max_heap + ". Using this value for -Xmx.")
        java_options += f" -Xmx{max_heap.split('=')[1]} "
    else:
        mem_mib = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / (1024. ** 2)
        max_memory = int(mem_mib * 0.8)  # 80% of system memory
        java_options += f" -Xmx{max_memory}m"
    if min_heap is not None:
        print("aerospike.graph-service.min-heap was set to " + min_heap + ". Using this value for -Xms.")
        java_options += f" -Xms{min_heap.split('=')[1]} "

    user_java_options = os.environ.get("JAVA_OPTIONS")
    if user_java_options is not None:
        print("Appending user provided JAVA_OPTIONS: " + user_java_options + " to java options.")
        java_options += user_java_options
    java_options += " --add-exports java.base/sun.nio.ch=ALL-UNNAMED "

    # Write classpath to file. Use 'w' to overwrite file.
    with open(java_options_file_path, "w") as java_options_file:
        java_options_file.write(java_options)

if __name__ == "__main__":
    input_properties_file = sys.argv[1]
    default_yaml_file = sys.argv[2]
    output_yaml_file = sys.argv[3]
    output_properties_file = sys.argv[4]
    output_java_options_file = sys.argv[5]
    try:
        main(input_properties_file, default_yaml_file, output_yaml_file, output_properties_file, output_java_options_file)
        sys.exit(0)
    except Exception as e:
        print(e)
        sys.exit(1)
