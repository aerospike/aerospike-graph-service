import os, sys, multiprocessing

def main(input_properties_file, default_yaml_file, output_yaml_file, output_properties_file, output_java_options_file, unified_config_file):
    valid_properties = []
    valid_yaml = []
    invalid = []
    java_options_max_heap = None
    java_options_min_heap = None
    auth_jwt_secret = None
    auth_jwt_issuer = None
    auth_jwt_algorithm = None
    performance_mode = None

    keys = []
    unified_config = []
    try:
        # May not be provided so try catch this block.
        with open(input_properties_file) as c:
            print("Reading properties file: " + input_properties_file)
            lines = [line.rstrip() for line in c]
            for line in lines:
                if line == "":
                    continue
                unified_config.append(line)
                if not "=" in line:
                    invalid.append(line)
                keys.append(line.split("=")[0])
                if line.startswith("aerospike.graph-service.heap.max"):
                    java_options_max_heap = line
                elif line.startswith("aerospike.graph-service.heap.min"):
                    java_options_min_heap = line
                elif line.startswith("aerospike.graph-service.auth.jwt.secret"):
                    auth_jwt_secret = line
                elif line.startswith("aerospike.graph-service.performance-mode"):
                    performance_mode = line
                elif line.startswith("aerospike.graph-service.auth.jwt.issuer"):
                    auth_jwt_issuer = line
                elif line.startswith("aerospike.graph-service.auth.jwt.algorithm"):
                    auth_jwt_algorithm = line
                elif line.startswith("aerospike.graph-service.auth.enabled"):
                    raise Exception("Error configuring Aerospike Graph Service.\n\t"
                                    "The property 'aerospike.graph-service.auth.enabled' is reserved.")
                elif line.startswith("aerospike.graph-service"):
                    valid_yaml.append(line)
                elif line.startswith("aerospike"):
                    valid_properties.append(line)
                elif not line.startswith("gremlin.graph"):
                    invalid.append(line)
    except Exception as e:
        # Do not allow auth.enabled to be set in properties file.
        if "'aerospike.graph-service.auth.enabled' is reserved" in str(e):
            raise e
        pass

    keys_in_both_properties_and_environment = []
    for key, value in os.environ.items():
        if key in keys:
            keys_in_both_properties_and_environment.append(key)

        if key.startswith("aerospike"):
            unified_config.append(f"{key}={value}")

        if key.startswith("aerospike.graph-service.heap.max"):
            java_options_max_heap = f"{key}={value}"
        elif key.startswith("aerospike.graph-service.heap.min"):
            java_options_min_heap = f"{key}={value}"
        elif key.startswith("aerospike.graph-service.auth.jwt.secret"):
            auth_jwt_secret = f"{key}={value}"
        elif key.startswith("aerospike.graph-service.performance-mode"):
            performance_mode = f"{key}={value}"
        elif key.startswith("aerospike.graph-service.auth.jwt.issuer"):
            auth_jwt_issuer = f"{key}={value}"
        elif key.startswith("aerospike.graph-service.auth.jwt.algorithm"):
            auth_jwt_algorithm = f"{key}={value}"
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

    persist_unified_config(unified_config_file, unified_config)
    generate_yaml(valid_yaml, default_yaml_file, output_yaml_file, output_properties_file, auth_jwt_secret, auth_jwt_issuer, auth_jwt_algorithm, performance_mode)
    generate_properties(valid_properties, output_properties_file, auth_jwt_secret, auth_jwt_issuer)
    generate_java_options(output_java_options_file, java_options_max_heap, java_options_min_heap)


def persist_unified_config(unified_config_file, unified_config):
    print("Persisting configuration to " + unified_config_file)
    with open(unified_config_file, "w") as unified_config_file:
        for line in unified_config:
            unified_config_file.write(line + "\n")
            if any(masked_keyword in line.split("=")[0].casefold() for masked_keyword in ['token', 'secret', 'password', 'passkey']):
                print("Persisting configuration: " + line.split("=")[0] + "=********")
            else :
                print("Persisting configuration: " + line)


def set_performance_mode(performance_mode, yaml_properties):
    # If performance mode is set, validate and assign values.
    if performance_mode is None:
        # If performance mode is not set and neither gremlinPool nor threadPoolWorker are set, default to throughput.
        thread_pool_worker_or_gremlin_pool_set = False
        for property in yaml_properties:
            if "aerospike.graph-service.threadPoolWorker" in property or "aerospike.graph-service.gremlinPool" in property:
                thread_pool_worker_or_gremlin_pool_set = True
                break
        if not thread_pool_worker_or_gremlin_pool_set:
            print("Defaulting 'aerospike.graph-service.performance-mode' to 'throughput'.")
            performance_mode = "aerospike.graph-service.performance-mode=throughput"
    else:
        for property in yaml_properties:
            if "aerospike.graph-service.threadPoolWorker" in property or "aerospike.graph-service.gremlinPool" in property:
                raise Exception("Error configuring Aerospike Graph Service.\n\t"
                                "Cannot set 'aerospike.graph-service.threadPoolWorker' or 'aerospike.graph-service.gremlinPool' "
                                "when using 'aerospike.graph-service.performance-mode'")

    if performance_mode is not None:
        performance_mode_value = performance_mode.split("=")[1]
        if performance_mode_value not in ["throughput", "latency"]:
            raise Exception("Error configuring Aerospike Graph Service.\n\t"
                            "Invalid value for 'aerospike.graph-service.performance-mode'. "
                            "Valid values are 'throughput' and 'latency'. "
                            "Provided value is '" + performance_mode_value + "'.")
        # Experiments show that throughput is best when gremlinPool=4*cpu_count and threadPoolWorker=cpu_count/2.
        # Latency is best when gremlinPool=cpu_count and threadPoolWorker=cpu_count/4.
        cpu_count = multiprocessing.cpu_count()
        if performance_mode_value == "throughput":
            thread_pool_worker = cpu_count//2
            gremlin_pool = 4*cpu_count
            if thread_pool_worker < 1:
                thread_pool_worker = 1
        else:
            thread_pool_worker = cpu_count//4
            if thread_pool_worker < 1:
                thread_pool_worker = 1
            gremlin_pool = cpu_count
        print("'aerospike.graph-service.performance-mode' is set to " + performance_mode_value + \
              ". Setting gremlinPool to " + str(gremlin_pool) + " and threadPoolWorker to " + str(thread_pool_worker) + ".")
        yaml_properties.append(f"aerospike.graph-service.gremlinPool={gremlin_pool}")
        yaml_properties.append(f"aerospike.graph-service.threadPoolWorker={thread_pool_worker}")


def generate_yaml(yaml_properties, default_yaml_file, output_yaml_file, output_properties_file, auth_jwt_secret, auth_jwt_issuer, auth_jwt_algorithm, performance_mode):
    rewritten_lines = []

    set_performance_mode(performance_mode, yaml_properties)

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
  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3, config: { ioRegistries: [org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3] }}            # application/json
  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1 }                                                                                                           # application/vnd.graphbinary-v1.0
  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1, config: { serializeResultToString: true }}                                                                 # application/vnd.graphbinary-v1.0-stringd
processors:
  - { className: org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor, config: { sessionTimeout: 28800000 }}
  - { className: org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor, config: { cacheExpirationTime: 600000, cacheMaxSize: 1000 }}
""")
    find_security_credentials(auth_jwt_secret, auth_jwt_issuer, auth_jwt_algorithm, rewritten_lines)

    lines = lines + rewritten_lines

    with open(output_yaml_file, "w") as yaml:
        for line in lines:
            yaml.write(line + "\n")

    with open(output_yaml_file, "r") as prop:
        output_yaml_str = prop.read()
        output_yaml_print = ""
        lines = output_yaml_str.split('\n')
        for line in lines:
            if "aerospike.graph-service.auth.jwt.secret" in line:
                output_yaml_print += "    aerospike.graph-service.auth.jwt.secret: ********\n"
            elif "aerospike.graph-service.auth.jwt.issuer" in line:
                output_yaml_print += "    aerospike.graph-service.auth.jwt.issuer: ********\n"
            else:
                output_yaml_print += line + "\n"
        print("Generated yaml file: " + output_yaml_file + "\n" + output_yaml_print)


def find_security_credentials(auth_jwt_secret, auth_jwt_issuer, auth_jwt_algorithm, rewritten_lines):
    secret = None
    algorithm = None
    issuer = None
    if auth_jwt_secret is not None:
        secret = auth_jwt_secret.split("=")[1]
    if auth_jwt_issuer is not None:
        issuer = auth_jwt_issuer.split("=")[1]
    if auth_jwt_algorithm is not None:
        algorithm = auth_jwt_algorithm.split("=")[1]

    if algorithm is not None and secret is None and issuer is None:
        raise Exception("Error configuring Aerospike Graph Service.\n\t"
                        "Configuring security requires both 'aerospike.graph-service.auth.jwt.secret' and "
                        "'aerospike.graph-service.auth.jwt.issuer' to be set, but only "
                        "'aerospike.graph-service.auth.jwt.algorithm' was set. "
                        "('aerospike.graph-service.auth.jwt.secret' and 'aerospike.graph-service.auth.jwt.issuer' are required).")
    if secret is not None and issuer is None:
        raise Exception("Error configuring Aerospike Graph Service.\n\t"
                        "Configuring security requires both 'aerospike.graph-service.auth.jwt.secret' and "
                        "'aerospike.graph-service.auth.jwt.issuer' to be set, but only "
                        "'aerospike.graph-service.auth.jwt.secret' was set. "
                        "('aerospike.graph-service.auth.jwt.algorithm' is optional).")
    elif issuer is not None and secret is None:
        raise Exception("Error configuring Aerospike Graph Service.\n\t"
                        "Configuring security requires both 'aerospike.graph-service.auth.jwt.secret' and "
                        "'aerospike.graph-service.auth.jwt.issuer' to be set, but only "
                        "'aerospike.graph-service.auth.jwt.issuer' was set. "
                        "('aerospike.graph-service.auth.jwt.algorithm' is optional).")
    elif issuer is not None and secret is not None:
        if algorithm is None:
            print("Defaulting 'aerospike.graph-service.auth.jwt.algorithm' to 'HMA256'.")
            algorithm = "HMAC256"
        elif algorithm not in ["HMAC256", "HMAC384", "HMAC512"]:
            raise Exception("Error configuring Aerospike Graph Service.\n\t"
                            "Invalid value for 'aerospike.graph-service.auth.jwt.algorithm'. "
                            "Valid values are 'HMAC256', 'HMAC384', and 'HMAC512'. "
                            "Provided value is '" + algorithm + "'.")
        rewritten_lines.append("""authentication: {
  authenticator: com.aerospike.firefly.security.JWTAuthenticator,
  config: {
    aerospike.graph-service.auth.jwt.secret: """ + secret + """,
    aerospike.graph-service.auth.jwt.issuer: """ + issuer)
        if algorithm is not None:
            rewritten_lines.append(""",
    aerospike.graph-service.auth.jwt.algorithm: """ + algorithm)
        rewritten_lines.append("""
  }
}
authorization: {
    authorizer: com.aerospike.firefly.security.JWTAuthorizer,
    config: {
    }
}
""")
    else:
        print("No security credentials found. Skipping security configuration.")


def generate_properties(properties, output_properties_file, auth_jwt_secret, auth_jwt_issuer):
    with open(output_properties_file, "w") as prop:
        if "gremlin.graph=com.aerospike.firefly.structure.FireflyGraph" not in properties:
            prop.write("gremlin.graph=com.aerospike.firefly.structure.FireflyGraph\n")
        for property in properties:
            prop.write(property + "\n")
        if auth_jwt_secret is not None and auth_jwt_issuer is not None:
            prop.write("aerospike.graph-service.auth.enabled=true\n")


def generate_java_options(java_options_file_path, max_heap, min_heap):
    java_options = ""

    # We are deprecating JAVA_OPTIONS in favor of using our notation. Users don't need to know we are using Java.
    if max_heap is not None:
        print("aerospike.graph-service.heap.max was set to " + max_heap + ". Using this value for -Xmx.")
        java_options += f" -Xmx{max_heap.split('=')[1]} "
    else:
        mem_mib = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / (1024. ** 2)
        max_memory = int(mem_mib * 0.8)  # 80% of system memory
        java_options += f" -Xmx{max_memory}m"
    if min_heap is not None:
        print("aerospike.graph-service.heap.min was set to " + min_heap + ". Using this value for -Xms.")
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
    unified_config_file = sys.argv[6]
    try:
        main(input_properties_file, default_yaml_file, output_yaml_file, output_properties_file, output_java_options_file, unified_config_file)
        sys.exit(0)
    except Exception as e:
        print(e)
        sys.exit(1)
