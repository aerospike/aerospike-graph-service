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

import os, sys, multiprocessing
import re
import yaml
import subprocess
import shutil


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

    ssl_dir = f"{conf_dir}ssl" if conf_dir.endswith("/") or conf_dir.endswith("\\") else f"{conf_dir}/ssl"
    generate_yaml(valid_yaml, default_yaml_file, output_yaml_file, graph_config, auth_jwt_secret, auth_jwt_issuer, auth_jwt_algorithm, ssl_dir, conf_dir)

    for key in named_graphs:
        # copy of common properties
        merged_properties = valid_properties.copy()
        for p in graph_config[key]:
            merged_properties.append(p[p.index(".")+1:])

        # let's check default graph_ID
        no_graph_id_provided = not any(s.startswith("aerospike.graph.id") for s in merged_properties)
        # but not for default `graph`
        if key != "graph" and no_graph_id_provided:
            merged_properties.append("aerospike.graph.id=" + key)

        generate_properties(merged_properties, f"{conf_dir}/aerospike-graph-{key}.properties", auth_jwt_secret, auth_jwt_issuer)

    generate_java_options(output_java_options_file, java_options_max_heap, java_options_min_heap, f"{conf_dir}/tls")

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


def generate_yaml(yaml_properties, default_yaml_file, output_yaml_file, graph_config, auth_jwt_secret, auth_jwt_issuer, auth_jwt_algorithm, ssl_out_dir, conf_dir):
    rewritten_lines = []

    console_reporter = {
        "enabled": "true",
        "interval": "180000"
    }
    csv_reporter = {
        "enabled": "false",
        "interval": "180000",
        "fileName": "/tmp/gremlin-server-metrics.csv"
    }
    jmx_reporter = {
        "enabled": "false"
    }
    slf4j_reporter = {
        "enabled": "false",
        "interval": "180000"
    }
    metrics = {
        "consoleReporter": console_reporter,
        "csvReporter": csv_reporter,
        "jmxReporter": jmx_reporter,
        "slf4jReporter": slf4j_reporter
    }

    default_keystore = ssl_out_dir + "/keystore.p12"
    default_keystore_password = "aerospike"
    ssl = {
        "enabled": "false",
        "keyStore": default_keystore,
        "keyStorePassword": default_keystore_password,
        "keyStoreType": "PKCS12"
    }

    set_performance_mode(yaml_properties)

    # Read yaml lines.
    with open(default_yaml_file) as default_yaml:
        lines = [line.rstrip() for line in default_yaml]

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
        elif key.startswith("metrics."):
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
        elif key.startswith("ssl"):
            if not key.startswith("ssl.") or len(key) < 5:
                raise Exception("Error configuring Aerospike Graph Service.\n\tSSL configurations must specify settings individually to be modified. Example: aerospike.graph-service.ssl.settingName=settingValue")
            key = key.replace("ssl.", "")
            ssl[key] = value
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

    # SSL (GLV Client<->AGS)
    rewritten_lines.append("ssl: { ")
    if ssl["enabled"].lower() != "true":
        rewritten_lines.append("    enabled: false")
    else:
        position = 1
        count = len(ssl)
        for ssl_key, ssl_value in ssl.items():
            if position == count:
                rewritten_lines.append(f"    {ssl_key}: {ssl_value}")
            else:
                rewritten_lines.append(f"    {ssl_key}: {ssl_value},")
            position += 1
        if ssl["keyStore"] == default_keystore and ssl["keyStorePassword"] == default_keystore_password:
            generate_server_keystore(ssl, ssl_out_dir)
    rewritten_lines.append("}")


    rewritten_lines.append("graphs: { ")
    for key in graph_config:
        if conf_dir.endswith("/") or conf_dir.endswith("\\"):
            rewritten_lines.append(f"  {key}: {conf_dir}aerospike-graph-{key}.properties,")
        else:
            rewritten_lines.append(f"  {key}: {conf_dir}/aerospike-graph-{key}.properties,")
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

    with open(output_yaml_file, "w") as default_yaml:
        for line in lines:
            default_yaml.write(line + "\n")

    with open(output_yaml_file, "r") as prop:
        output_yaml_str = prop.read()
        output_yaml_print = ""
        lines = output_yaml_str.split('\n')
        for line in lines:
            if "password" in line.lower():
                split = line.split(":", 1)
                output_yaml_print += split[0] + ": ********,\n"
            elif "aerospike.graph-service.auth.jwt.secret" in line.lower():
                output_yaml_print += "    aerospike.graph-service.auth.jwt.secret: ********,\n"
            elif "aerospike.graph-service.auth.jwt.issuer" in line.lower():
                output_yaml_print += "    aerospike.graph-service.auth.jwt.issuer: ********,\n"
            else:
                output_yaml_print += line + "\n"
        print("Generated yaml file: " + output_yaml_file + "\n" + output_yaml_print)


def generate_server_keystore(ssl_options, ssl_out_dir):
    keystore_dir = "/opt/aerospike-graph/gremlin-server-tls"
    ca_dir = "opt/aerospike-graph/gremlin-server-ca"
    certificate = None
    alias_name = None
    private_key = None
    ca = None
    if os.path.isdir(keystore_dir):
        keystore_files = os.listdir(os.fsencode(keystore_dir))
        if len(keystore_files) != 2:
            message = f"An unexpected number of files was detected in setup directory for Gremlin Client SSL. Please ensure only the certificate and private key files are mounted to: {keystore_dir}"
            print(message)
            raise Exception(message)
        for file in keystore_files:
            file_name = os.fsdecode(file)
            print(f"Found file to use for Gremlin Client SSL: {file_name}")
            full_file_path = keystore_dir + "/" + file_name
            with open(full_file_path, "r") as f:
                content = f.read().lower()
                if "private key" in content:
                    print(f"{full_file_path} set as Private Key for Gremlin Client SSL.")
                    private_key = full_file_path
                elif "certificate" in content:
                    print(f"{full_file_path} set as Certificate for Gremlin Client SSL.")
                    certificate = full_file_path
                    alias_name = os.path.splitext(file_name)[0]
                else:
                    message = f"{full_file_path} is not a valid Private Key or Certificate file."
                    print(message)
                    raise Exception(message)
        if certificate is None or private_key is None:
            message = "Setting up Gremlin Client SSL failed - did not find a valid Private Key and Certificate file."
            print(message)
            raise Exception(message)
    else:
        message = f"Gremlin Client SSL was set to enabled but no files for use were found at: {keystore_dir}"
        print(message)
        raise Exception(message)

    if os.path.isdir(ca_dir):
        ca_files = os.listdir(os.fsencode(ca_dir))
        if len(ca_files) != 1:
            message = f"More than one Certificate Authority file was detected in the setup directory for Gremlin Client SSL. Please ensure only one file is mounted to: {ca_dir}"
            print(message)
            raise Exception(message)
        ca_file_name = os.fsdecode(ca_files[0])
        print(f"Found CA file to use for Gremlin Client SSL: {ca_file_name}")
        ca = ca_dir + "/" + ca_file_name
    if os.path.exists(ssl_out_dir):
        shutil.rmtree(ssl_out_dir)
    os.makedirs(ssl_out_dir, exist_ok=True)
    cmd = [
        "openssl", "pkcs12", "-export",
        "-in", certificate,
        "-inkey", private_key,
        "-out", ssl_options["keyStore"],
        "-name", alias_name,
        "-passout", f'pass:{ssl_options["keyStorePassword"]}'
    ]
    if ca:
        cmd.extend(["-certfile", ca])
    try:
        subprocess.run(cmd, check=True)
    except Exception as openssl_exception:
        print(f"Generating keystore with openssl for Gremlin Client SSL failed: {openssl_exception}")
        raise openssl_exception


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
    aerospike.graph-service.auth.jwt.secret: """ + secret + ",")
        if algorithm is not None:
            rewritten_lines.append("""    aerospike.graph-service.auth.jwt.issuer: """ + issuer + ",")
            rewritten_lines.append("""    aerospike.graph-service.auth.jwt.algorithm: """ + algorithm)
        else:
            rewritten_lines.append("""    aerospike.graph-service.auth.jwt.issuer: """ + issuer)
        rewritten_lines.append("""  }
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


def _find_system_cacerts():
    """Locate the JVM's default CA truststore (cacerts).

    Checks Alpine paths first, then JAVA_HOME, then walks common JVM
    directories so this works on Debian/Ubuntu, RHEL/Amazon Linux, etc.
    """
    java_home = os.environ.get("JAVA_HOME", "")
    candidates = [
        # Alpine (primary target)
        "/etc/ssl/certs/java/cacerts",
        "/usr/lib/jvm/java-17-openjdk/lib/security/cacerts",
        # JAVA_HOME-based (works when the env var is set)
        os.path.join(java_home, "lib", "security", "cacerts"),
        os.path.join(java_home, "jre", "lib", "security", "cacerts"),
    ]
    for path in candidates:
        if path and os.path.isfile(path):
            return path
    # Fallback: walk common JVM/cert directories
    for root in ["/usr/lib/jvm", "/etc/pki/java", "/etc/ssl/certs/java"]:
        if os.path.isdir(root):
            for dirpath, _, filenames in os.walk(root):
                if "cacerts" in filenames:
                    path = os.path.join(dirpath, "cacerts")
                    if os.path.isfile(path):
                        return path
    return None


def generate_java_options(java_options_file_path, max_heap, min_heap, tls_out_dir):
    java_options = ""

    # We are deprecating JAVA_OPTIONS in favor of using our notation. Users don't need to know we are using Java.
    if max_heap is not None:
        print("aerospike.graph-service.heap.max was set to " + max_heap + ". Using this value for -Xmx.")
        java_options += f" -Xmx{max_heap.split('=')[1]} "
    else:
        java_options += " -XX:MaxRAMPercentage=80.0 "
    if min_heap is not None:
        print("aerospike.graph-service.heap.min was set to " + min_heap + ". Using this value for -Xms.")
        java_options += f" -Xms{min_heap.split('=')[1]} "

    user_java_options = os.environ.get("JAVA_OPTIONS")
    if user_java_options is not None:
        lower_options = user_java_options.lower()
        password_indexes = [(i+9) for i in range(len(lower_options)) if lower_options.startswith('password=', i)]
        printable_options = ''
        password_character = False
        for i in range(len(user_java_options)):
            if i in password_indexes:
                if password_character:
                    # This should never happen unless the environment variable is malformed.
                    raise Exception("Unexpected problem when parsing JAVA_OPTIONS from environment variables. Please check the format and retry.")
                password_character = True
            if password_character and user_java_options[i] == ' ':
                password_character = False

            if password_character:
                printable_options += '*'
            else:
                printable_options += user_java_options[i]

        print("Appending user provided JAVA_OPTIONS: " + printable_options + " to java options.")
        java_options += user_java_options

    # Set up TLS for AGS<->Aerospike DB
    # The truststore is seeded from the system CA bundle (cacerts) so that public
    # CAs remain trusted alongside the Aerospike CA. Without this, any outbound
    # HTTPS (e.g. S3 bulk load) fails with PKIX path building errors.
    cert_dir = "/opt/aerospike-graph/aerospike-client-tls"
    cert_found = False
    if os.path.isdir(cert_dir):
        directory = os.fsencode(cert_dir)
        if os.path.exists(tls_out_dir):
            shutil.rmtree(tls_out_dir)
        os.makedirs(tls_out_dir, exist_ok=True)
        keystore = tls_out_dir + "/truststore.jks"
        storepass = "aerospike"

        system_cacerts = _find_system_cacerts()
        if system_cacerts:
            # NOTE: "changeit" is the JDK's standard, well-known default password for the
            # system CA truststore (cacerts). It is NOT a project secret or a placeholder to
            # harden - it is a fixed property of the JVM. It is only used here to READ the
            # source cacerts; the destination truststore is written with our own password
            # (storepass). Changing this value would break reading the system CA bundle.
            result = subprocess.run([
                "keytool", "-importkeystore",
                "-srckeystore", system_cacerts,
                "-destkeystore", keystore,
                "-srcstorepass", "changeit",
                "-deststorepass", storepass,
                "-noprompt"
            ], check=False)
            if result.returncode == 0:
                print(f"Seeded truststore from system CAs: {system_cacerts}")
            else:
                print(f"WARNING: Failed to seed truststore from system CAs (unexpected cacerts password?). "
                      f"Truststore will only contain Aerospike CA.")
        else:
            print("WARNING: System cacerts not found. Truststore will only contain Aerospike CA.")

        for file in os.listdir(directory):
            file_name = os.fsdecode(file)
            print("Found file to use for Aerospike Database TLS: " + file_name)
            cmd = [
                "keytool",
                "-import",
                "-trustcacerts",
                "-noprompt",
                "-alias", os.path.splitext(file_name)[0],
                "-file", cert_dir + "/" + file_name,
                "-keystore", keystore,
                "-storepass", storepass,
            ]

            try:
                subprocess.run(cmd, check=True)
                cert_found = True
            except Exception as keytool_exception:
                print(f"Generating truststore with keytool for Aerospike Database TLS failed: {keytool_exception}")
                raise keytool_exception
        if cert_found:
            java_options += f" -Djavax.net.ssl.trustStore={keystore} -Djavax.net.ssl.trustStorePassword={storepass} "
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
