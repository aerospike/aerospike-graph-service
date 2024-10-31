import os, sys, multiprocessing
import re


def main(input_properties_file, default_yaml_file, output_yaml_file, conf_dir, output_java_options_file):

    valid_properties = []
    valid_yaml = []
    invalid = []
    java_options_max_heap = None
    java_options_min_heap = None
    auth_jwt_secret = None
    auth_jwt_issuer = None
    auth_jwt_algorithm = None

    named_graphs = []
    # configuration specific to each graph
    graph_config = {}

    # handle graph names before everything.
    for key, value in os.environ.items():
        if key.lower() == "aerospike.graph-service.graphs":
            named_graphs = list(map(str.strip, value.split(",")))

    try:
        # May not be provided so try catch this block.
        with open(input_properties_file) as c:
            print("Reading properties file: " + input_properties_file)
            lines = [line.rstrip() for line in c]

            # no named graphs from environment variables, so let's try to search in properties file.
            if len(named_graphs) == 0:
                for line in lines:
                    if line.startswith("aerospike.graph-service.graphs") and "=" in line:
                        named_graphs = list(map(str.strip, (line.split("=")[1]).split(",")))

            for line in lines:
                if line == "" or line.startswith("#"):
                    continue
                if not "=" in line:
                    invalid.append(line)
                elif line.startswith("aerospike.graph-service.graphs"):
                    continue
                elif line.startswith("aerospike.graph-service.heap.max"):
                    java_options_max_heap = line
                elif line.startswith("aerospike.graph-service.heap.min"):
                    java_options_min_heap = line
                elif line.startswith("aerospike.graph-service.auth.jwt.secret"):
                    auth_jwt_secret = line
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
                elif line.split(".")[0] in named_graphs:
                    k = line.split(".")[0]
                    if not k in graph_config:
                        graph_config[k] = []
                    graph_config[k].append(line)
                elif not line.startswith("gremlin.graph"):
                    invalid.append(line)
    except Exception as e:
        # Do not allow auth.enabled to be set in properties file.
        if "'aerospike.graph-service.auth.enabled' is reserved" in str(e):
            raise e
        pass

    print("Found named graphs: " + str(named_graphs))

    for key, value in os.environ.items():
        if key.lower() == "aerospike.graph-service.graphs":
            continue
        elif key.startswith("aerospike.graph-service.heap.max"):
            java_options_max_heap = f"{key}={value}"
        elif key.startswith("aerospike.graph-service.heap.min"):
            java_options_min_heap = f"{key}={value}"
        elif key.startswith("aerospike.graph-service.auth.jwt.secret"):
            auth_jwt_secret = f"{key}={value}"
        elif key.startswith("aerospike.graph-service.auth.jwt.issuer"):
            auth_jwt_issuer = f"{key}={value}"
        elif key.startswith("aerospike.graph-service.auth.jwt.algorithm"):
            auth_jwt_algorithm = f"{key}={value}"
        elif key.startswith("aerospike.graph-service"):
            valid_yaml.append(f"{key}={value}")
        elif key.split(".")[0] in named_graphs:
            k = key.split(".")[0]
            if not k in graph_config:
                graph_config[k] = []
            graph_config[k].append(f"{key}={value}")
        elif key.startswith("aerospike"):
            valid_properties.append(f"{key}={value}")

    if len(invalid) > 0:
        raise Exception("Error configuring Aerospike Graph Service.\n\tInvalid properties found: " + str(invalid) + ". Properties must start with 'aerospike' and " + \
                    "be in the format 'aerospike.key=value'")

    # add default graph unless otherwise explicitly stated
    if len(named_graphs) == 0:
        named_graphs = ["graph"]

    # graph name validation
    for graph_name in named_graphs:
        if not re.match('[A-Za-z0-9_-]+$', graph_name):
            raise Exception(f"Graph name should be within [a-z][A-Z][0-9][-_], but found {graph_name}")
        if len(graph_name) > 32:
            raise Exception(f"Length of graph name shall be less then 32 characters, but found {graph_name}")

    for key in named_graphs:
        if key not in graph_config:
            graph_config[key] = []

    generate_yaml(valid_yaml, default_yaml_file, output_yaml_file, graph_config, auth_jwt_secret, auth_jwt_issuer, auth_jwt_algorithm)

    for key in named_graphs:
        merged_properties = valid_properties
        for p in graph_config[key]:
            merged_properties.append(p[p.index(".")+1:])

        # let's check default graph_ID
        no_graph_id_provided = not any(s.startswith("aerospike.graph.id") for s in merged_properties)
        # but not for default `graph`
        if key != "graph" and no_graph_id_provided:
            merged_properties.append("aerospike.graph.id=" + key)

        generate_properties(merged_properties, f"{conf_dir}/aerospike-graph-{key}.properties", auth_jwt_secret, auth_jwt_issuer)

    generate_java_options(output_java_options_file, java_options_max_heap, java_options_min_heap)

def set_performance_mode(yaml_properties):
    # Experiments show that throughput is best when gremlinPool=4*cpu_count and threadPoolWorker=cpu_count/2.
    # Latency is best when gremlinPool=cpu_count and threadPoolWorker=cpu_count/4.
    cpu_count = multiprocessing.cpu_count()
    thread_pool_worker = cpu_count//2
    if thread_pool_worker < 1:
        thread_pool_worker = 1
    gremlin_pool = 4*cpu_count

    # Shouldn't happen but it's unclear what would happen if someone allocates 1/8 of a CPU or something.
    if gremlin_pool < 1:
        gremlin_pool = 1

    found_thread_pool_worker = False
    found_gremlin_pool = False
    for property in yaml_properties:
        if "aerospike.graph-service.threadPoolWorker" in property:
            found_thread_pool_worker = True
            thread_pool_worker = int(property.split("=")[1])
        if "aerospike.graph-service.gremlinPool" in property:
            found_gremlin_pool = True
            gremlin_pool = int(property.split("=")[1])

    if not found_thread_pool_worker:
        yaml_properties.append(f"aerospike.graph-service.threadPoolWorker={thread_pool_worker}")
    if not found_gremlin_pool:
        yaml_properties.append(f"aerospike.graph-service.gremlinPool={gremlin_pool}")

    print("Setting gremlinPool to " + str(gremlin_pool) + " and threadPoolWorker to " + str(thread_pool_worker) + ".")


def generate_yaml(yaml_properties, default_yaml_file, output_yaml_file, graph_config, auth_jwt_secret, auth_jwt_issuer, auth_jwt_algorithm):
    rewritten_lines = []

    console_reporter = {
        "enabled": "true",
        "interval": "180000"
    }
    csv_reporter = {
        "enabled": "true",
        "interval": "180000",
        "fileName": "/tmp/gremlin-server-metrics.csv"
    }
    jmx_reporter = {
        "enabled": "true"
    }
    slf4j_reporter = {
        "enabled": "true",
        "interval": "180000"
    }
    metrics = {
        "consoleReporter": console_reporter,
        "csvReporter": csv_reporter,
        "jmxReporter": jmx_reporter,
        "slf4jReporter": slf4j_reporter
    }

    set_performance_mode(yaml_properties)

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
        if key.startswith("metrics."):
            key = key.replace("metrics.", "")
            if key.split(".")[0] in metrics:
                metrics_key = key.split(".")[0]
                reporter = metrics.get(metrics_key)
                key = key.replace(metrics_key + ".", "")
                if key in reporter:
                    reporter[key] = value
                else:
                    raise Exception("Error configuring Aerospike Graph Service.\n\t" + key +
                                    " is not a valid configuration for metrics of type " + metrics_key + ".")
            else:
                raise Exception(
                    "Error configuring Aerospike Graph Service.\n\t" + key.split(".")[0] + \
                    " is not a valid metrics type.")
        else:
            lines = [i for i in lines if not i.startswith(key)]
            rewritten_lines.append(f"{key}: {value}")

    # Metrics
    rewritten_lines.append("metrics: { ")
    metrics_count = len(metrics)
    metrics_position = 1
    for reporter_name, reporter in metrics.items():
        rewritten_lines.append(f"  {reporter_name}:" + " { ")
        reporter_count = len(reporter)
        reporter_position = 1
        for setting_name, setting_value in reporter.items():
            if reporter_position == reporter_count:
                rewritten_lines.append(f"    {setting_name}: {setting_value}")
            else:
                rewritten_lines.append(f"    {setting_name}: {setting_value},")
            reporter_position += 1
        if metrics_position == metrics_count:
            rewritten_lines.append("  }")
        else:
            rewritten_lines.append("  },")
        metrics_position += 1
    rewritten_lines.append("}")

    rewritten_lines.append("graphs: { ")
    for key in graph_config:
        rewritten_lines.append(f"  {key}: conf/aerospike-graph-{key}.properties,")
    rewritten_lines.append("}")

    # Pop serializers in here since we can't flatten them.
    rewritten_lines.append(
"""serializers:
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

        added_properties = []
        for property in reversed(properties):
            prop_name = property[:property.index("=")]
            if not prop_name in added_properties:
                prop.write(property + "\n")
                added_properties.append(prop_name)

        if auth_jwt_secret is not None and auth_jwt_issuer is not None:
            prop.write("aerospike.graph-service.auth.enabled=true\n")


def generate_java_options(java_options_file_path, max_heap, min_heap):
    java_options = ""

    # We are deprecating JAVA_OPTIONS in favor of using our notation. Users don't need to know we are using Java.
    if max_heap is not None:
        print("aerospike.graph-service.heap.max was set to " + max_heap + ". Using this value for -Xmx.")
        java_options += f" -Xmx{max_heap.split('=')[1]} "
    else:
        try:
            mem_mib = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / (1024. ** 2)
        except Exception as e:
            # to run script on windows
            mem_mib = 1024.
        max_memory = int(mem_mib * 0.8)  # 80% of system memory
        java_options += f" -Xmx{max_memory}m "
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
    output_conf_dir = sys.argv[4]
    output_java_options_file = sys.argv[5]

    try:
        main(input_properties_file, default_yaml_file, output_yaml_file, output_conf_dir, output_java_options_file)
        sys.exit(0)
    except Exception as e:
        print(e)
        sys.exit(1)
