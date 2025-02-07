package com.aerospike.firefly.olap;

import com.aerospike.firefly.olap.helper.CommandLineParser;
import com.aerospike.firefly.runtime.FireflyServer;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.aerospike.firefly.olap.helper.CommandLineParser.CONFIG_DIRECTORY_KEY;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * Most of the Distributed* classes are adapted from the Spark* in TinkerPop from by Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DistributedGraphComputerMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedGraphComputerMain.class);
    public static final String LOCAL = "local";
    public static final String S3 = "s3";
    public static final String GCS = "gcs";
    static String fileSystem = LOCAL;


    public static final String LOCAL_MODE = "local";
    public static final String GCS_KEYFILE_DIRECTORY = "aerospike.graphloader.gcs-keyfile";
    public static final String GCS_EMAIL = "aerospike.graphloader.gcs-email";
    public static final String REMOTE_USERNAME = "aerospike.graphloader.remote-user";
    public static final String REMOTE_PASSKEY = "aerospike.graphloader.remote-passkey";


    public static void main(final String[] args) {

        // Create new Object so we can invoke non-static method load()
        LOGGER.info("Starting Aerospike Graph OLAP Server");

        System.out.println();
        System.out.println("       +-----------------------------------------------------+      ");
        System.out.println("       |              AEROSPIKE GRAPH OLAP                   |      ");
        System.out.println("       +-----------------------------------------------------+      ");
        System.out.println("                   |                             |                  ");
        System.out.println("        +----------------------+        +-------------------+       ");
        System.out.println("        |     APACHE SPARK     |<------>|     TINKERPOP     |       ");
        System.out.println("        +----------------------+        +-------------------+       ");
        System.out.println();

        CommandLine commandLine = CommandLineParser.parseCmdArgs(args);
        commandLine.getOptionValue("c");

        // Need to write flattened server yaml to a file so python can configure it.
        final String serverYaml = getServerYaml();
        final String tempDirectory = System.getProperty("java.io.tmpdir") + "/";
        final String serverYamlPath = tempDirectory + "flattened-default-gremlin-server.yaml";

        // Write the server yaml to a file.
        try {
            FileUtils.writeStringToFile(new File(serverYamlPath), serverYaml, StandardCharsets.UTF_8);
        } catch (Exception e) {
            LOGGER.error("Failed to write default server yaml to file for configuration: " + serverYamlPath, e);
            System.exit(1);
        }

        final String outputServerYamlPath = tempDirectory + "gremlin-server.yaml";
        final SparkSession spark = buildSparkSession(commandLine);
        final String configPath = commandLine.hasOption("c") ? commandLine.getOptionValue("c") : null;
        configureFileSystem(spark, commandLine, commandLine.getOptionValue(CONFIG_DIRECTORY_KEY));
        final Map<String, Object> fileConfig = loadConfiguration(spark, commandLine, configPath);

        // Write config file to a file so python can configure it.
        final String configFilePath = tempDirectory + "aerospike-graph.properties";
        final StringBuilder configMapToFile = new StringBuilder();
        for (final Map.Entry<String, Object> entry : fileConfig.entrySet()) {
            configMapToFile.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }
        try {
            FileUtils.writeStringToFile(new File(configFilePath), configMapToFile.toString(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            LOGGER.error("Failed to write config file to file for configuration: " + configFilePath, e);
            System.exit(1);
        }

        // Write python script to temp file.
        final String pythonScript = getPythonScript();
        final String pythonScriptPath = tempDirectory + "configure_aerospike_graph.py";
        try {
            FileUtils.writeStringToFile(new File(pythonScriptPath), pythonScript, StandardCharsets.UTF_8);
        } catch (Exception e) {
            LOGGER.error("Failed to write python script to file for configuration: " + pythonScriptPath, e);
            System.exit(1);
        }

        final String outputServerYaml = tempDirectory + "gremlin-server.yaml";

        // Invoke python
        try {
            final ProcessBuilder processBuilder = new ProcessBuilder("python3",
                    pythonScriptPath,
                    configFilePath,
                    serverYamlPath,
                    outputServerYaml,
                    tempDirectory,
                    tempDirectory + "/java_options.txt");
            final Process process = processBuilder.start();
            final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            LOGGER.error("Failed to invoke python script for configuration.", e);
            System.exit(1);
        }

        FireflyServer fireflyServer = FireflyServer.start(List.of(outputServerYaml).toArray(new String[]{}));
    }

    private static String getServerYaml() {
        return "host: 0.0.0.0\n" +
                "port: 8182\n" +
                "evaluationTimeout: 10000\n" +
                "channelizer: org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer\n" +
                "scriptEngines: {}\n" +
                "strictTransactionManagement: false\n" +
                "idleConnectionTimeout: 0\n" +
                "keepAliveInterval: 0\n" +
                "maxInitialLineLength: 4096\n" +
                "maxHeaderSize: 8192\n" +
                "maxChunkSize: 8192\n" +
                "maxContentLength: 10485760\n" +
                "maxAccumulationBufferComponents: 1024\n" +
                "resultIterationBatchSize: 64\n" +
                "writeBufferLowWaterMark: 32768\n" +
                "writeBufferHighWaterMark: 65536\n" +
                "ssl: { enabled: false }\n";
    }

    private static String getPythonScript() {
        return "import os, sys, multiprocessing\n" +
                "import re\n" +
                "\n" +
                "\n" +
                "def main(input_properties_file, default_yaml_file, output_yaml_file, conf_dir, output_java_options_file):\n" +
                "\n" +
                "    valid_properties = []\n" +
                "    valid_yaml = []\n" +
                "    invalid = []\n" +
                "    java_options_max_heap = None\n" +
                "    java_options_min_heap = None\n" +
                "    auth_jwt_secret = None\n" +
                "    auth_jwt_issuer = None\n" +
                "    auth_jwt_algorithm = None\n" +
                "\n" +
                "    named_graphs = []\n" +
                "    # configuration specific to each graph\n" +
                "    graph_config = {}\n" +
                "\n" +
                "    # handle graph names before everything.\n" +
                "    for key, value in os.environ.items():\n" +
                "        if key.lower() == \"aerospike.graph-service.graphs\":\n" +
                "            named_graphs = list(map(str.strip, value.split(\",\")))\n" +
                "\n" +
                "    try:\n" +
                "        # May not be provided so try catch this block.\n" +
                "        with open(input_properties_file) as c:\n" +
                "            print(\"Reading properties file: \" + input_properties_file)\n" +
                "            lines = [line.rstrip() for line in c]\n" +
                "\n" +
                "            # no named graphs from environment variables, so let's try to search in properties file.\n" +
                "            if len(named_graphs) == 0:\n" +
                "                for line in lines:\n" +
                "                    if line.startswith(\"aerospike.graph-service.graphs\") and \"=\" in line:\n" +
                "                        named_graphs = list(map(str.strip, (line.split(\"=\")[1]).split(\",\")))\n" +
                "\n" +
                "            for line in lines:\n" +
                "                if line == \"\" or line.startswith(\"#\"):\n" +
                "                    continue\n" +
                "                if not \"=\" in line:\n" +
                "                    invalid.append(line)\n" +
                "                elif line.startswith(\"aerospike.graph-service.graphs\"):\n" +
                "                    continue\n" +
                "                elif line.startswith(\"aerospike.graph-service.heap.max\"):\n" +
                "                    java_options_max_heap = line\n" +
                "                elif line.startswith(\"aerospike.graph-service.heap.min\"):\n" +
                "                    java_options_min_heap = line\n" +
                "                elif line.startswith(\"aerospike.graph-service.auth.jwt.secret\"):\n" +
                "                    auth_jwt_secret = line\n" +
                "                elif line.startswith(\"aerospike.graph-service.auth.jwt.issuer\"):\n" +
                "                    auth_jwt_issuer = line\n" +
                "                elif line.startswith(\"aerospike.graph-service.auth.jwt.algorithm\"):\n" +
                "                    auth_jwt_algorithm = line\n" +
                "                elif line.startswith(\"aerospike.graph-service.auth.enabled\"):\n" +
                "                    raise Exception(\"Error configuring Aerospike Graph Service.\\n\\t\"\n" +
                "                                    \"The property 'aerospike.graph-service.auth.enabled' is reserved.\")\n" +
                "                elif line.startswith(\"aerospike.graph-service\"):\n" +
                "                    valid_yaml.append(line)\n" +
                "                elif line.startswith(\"aerospike\"):\n" +
                "                    valid_properties.append(line)\n" +
                "                elif line.split(\".\")[0] in named_graphs:\n" +
                "                    k = line.split(\".\")[0]\n" +
                "                    if not k in graph_config:\n" +
                "                        graph_config[k] = []\n" +
                "                    graph_config[k].append(line)\n" +
                "                elif not line.startswith(\"gremlin.graph\"):\n" +
                "                    invalid.append(line)\n" +
                "    except Exception as e:\n" +
                "        # Do not allow auth.enabled to be set in properties file.\n" +
                "        if \"'aerospike.graph-service.auth.enabled' is reserved\" in str(e):\n" +
                "            raise e\n" +
                "        pass\n" +
                "\n" +
                "    print(\"Found named graphs: \" + str(named_graphs))\n" +
                "\n" +
                "    for key, value in os.environ.items():\n" +
                "        if key.lower() == \"aerospike.graph-service.graphs\":\n" +
                "            continue\n" +
                "        elif key.startswith(\"aerospike.graph-service.heap.max\"):\n" +
                "            java_options_max_heap = f\"{key}={value}\"\n" +
                "        elif key.startswith(\"aerospike.graph-service.heap.min\"):\n" +
                "            java_options_min_heap = f\"{key}={value}\"\n" +
                "        elif key.startswith(\"aerospike.graph-service.auth.jwt.secret\"):\n" +
                "            auth_jwt_secret = f\"{key}={value}\"\n" +
                "        elif key.startswith(\"aerospike.graph-service.auth.jwt.issuer\"):\n" +
                "            auth_jwt_issuer = f\"{key}={value}\"\n" +
                "        elif key.startswith(\"aerospike.graph-service.auth.jwt.algorithm\"):\n" +
                "            auth_jwt_algorithm = f\"{key}={value}\"\n" +
                "        elif key.startswith(\"aerospike.graph-service\"):\n" +
                "            valid_yaml.append(f\"{key}={value}\")\n" +
                "        elif key.split(\".\")[0] in named_graphs:\n" +
                "            k = key.split(\".\")[0]\n" +
                "            if not k in graph_config:\n" +
                "                graph_config[k] = []\n" +
                "            graph_config[k].append(f\"{key}={value}\")\n" +
                "        elif key.startswith(\"aerospike\"):\n" +
                "            valid_properties.append(f\"{key}={value}\")\n" +
                "\n" +
                "    if len(invalid) > 0:\n" +
                "        raise Exception(\"Error configuring Aerospike Graph Service.\\n\\tInvalid properties found: \" + str(invalid) + \". Properties must start with 'aerospike' and \" + \\\n" +
                "                    \"be in the format 'aerospike.key=value'\")\n" +
                "\n" +
                "    # add default graph unless otherwise explicitly stated\n" +
                "    if len(named_graphs) == 0:\n" +
                "        named_graphs = [\"graph\"]\n" +
                "\n" +
                "    # graph name validation\n" +
                "    for graph_name in named_graphs:\n" +
                "        if not re.match('[A-Za-z0-9_-]+$', graph_name):\n" +
                "            raise Exception(f\"Graph name should be within [a-z][A-Z][0-9][-_], but found {graph_name}\")\n" +
                "        if len(graph_name) > 32:\n" +
                "            raise Exception(f\"Length of graph name shall be less then 32 characters, but found {graph_name}\")\n" +
                "\n" +
                "    for key in named_graphs:\n" +
                "        if key not in graph_config:\n" +
                "            graph_config[key] = []\n" +
                "\n" +
                "    generate_yaml(valid_yaml, default_yaml_file, output_yaml_file, graph_config, auth_jwt_secret, auth_jwt_issuer, auth_jwt_algorithm, conf_dir)\n" +
                "\n" +
                "    for key in named_graphs:\n" +
                "        # copy of common properties\n" +
                "        merged_properties = valid_properties.copy()\n" +
                "        for p in graph_config[key]:\n" +
                "            merged_properties.append(p[p.index(\".\")+1:])\n" +
                "\n" +
                "        # let's check default graph_ID\n" +
                "        no_graph_id_provided = not any(s.startswith(\"aerospike.graph.id\") for s in merged_properties)\n" +
                "        # but not for default `graph`\n" +
                "        if key != \"graph\" and no_graph_id_provided:\n" +
                "            merged_properties.append(\"aerospike.graph.id=\" + key)\n" +
                "\n" +
                "        generate_properties(merged_properties, f\"{conf_dir}/aerospike-graph-{key}.properties\", auth_jwt_secret, auth_jwt_issuer)\n" +
                "\n" +
                "    generate_java_options(output_java_options_file, java_options_max_heap, java_options_min_heap)\n" +
                "\n" +
                "def set_performance_mode(yaml_properties):\n" +
                "    # Experiments show that throughput is best when gremlinPool=4*cpu_count and threadPoolWorker=cpu_count/2.\n" +
                "    # Latency is best when gremlinPool=cpu_count and threadPoolWorker=cpu_count/4.\n" +
                "    cpu_count = multiprocessing.cpu_count()\n" +
                "    thread_pool_worker = cpu_count//2\n" +
                "    if thread_pool_worker < 1:\n" +
                "        thread_pool_worker = 1\n" +
                "    gremlin_pool = 4*cpu_count\n" +
                "\n" +
                "    # Shouldn't happen but it's unclear what would happen if someone allocates 1/8 of a CPU or something.\n" +
                "    if gremlin_pool < 1:\n" +
                "        gremlin_pool = 1\n" +
                "\n" +
                "    found_thread_pool_worker = False\n" +
                "    found_gremlin_pool = False\n" +
                "    for property in yaml_properties:\n" +
                "        if \"aerospike.graph-service.threadPoolWorker\" in property:\n" +
                "            found_thread_pool_worker = True\n" +
                "            thread_pool_worker = int(property.split(\"=\")[1])\n" +
                "        if \"aerospike.graph-service.gremlinPool\" in property:\n" +
                "            found_gremlin_pool = True\n" +
                "            gremlin_pool = int(property.split(\"=\")[1])\n" +
                "\n" +
                "    if not found_thread_pool_worker:\n" +
                "        yaml_properties.append(f\"aerospike.graph-service.threadPoolWorker={thread_pool_worker}\")\n" +
                "    if not found_gremlin_pool:\n" +
                "        yaml_properties.append(f\"aerospike.graph-service.gremlinPool={gremlin_pool}\")\n" +
                "\n" +
                "    print(\"Setting gremlinPool to \" + str(gremlin_pool) + \" and threadPoolWorker to \" + str(thread_pool_worker) + \".\")\n" +
                "\n" +
                "\n" +
                "def generate_yaml(yaml_properties, default_yaml_file, output_yaml_file, graph_config, auth_jwt_secret, auth_jwt_issuer, auth_jwt_algorithm, conf_dir):\n" +
                "    rewritten_lines = []\n" +
                "\n" +
                "    console_reporter = {\n" +
                "        \"enabled\": \"true\",\n" +
                "        \"interval\": \"180000\"\n" +
                "    }\n" +
                "    csv_reporter = {\n" +
                "        \"enabled\": \"true\",\n" +
                "        \"interval\": \"180000\",\n" +
                "        \"fileName\": \"/tmp/gremlin-server-metrics.csv\"\n" +
                "    }\n" +
                "    jmx_reporter = {\n" +
                "        \"enabled\": \"true\"\n" +
                "    }\n" +
                "    slf4j_reporter = {\n" +
                "        \"enabled\": \"true\",\n" +
                "        \"interval\": \"180000\"\n" +
                "    }\n" +
                "    metrics = {\n" +
                "        \"consoleReporter\": console_reporter,\n" +
                "        \"csvReporter\": csv_reporter,\n" +
                "        \"jmxReporter\": jmx_reporter,\n" +
                "        \"slf4jReporter\": slf4j_reporter\n" +
                "    }\n" +
                "\n" +
                "    set_performance_mode(yaml_properties)\n" +
                "\n" +
                "    # Read yaml lines.\n" +
                "    with open(default_yaml_file) as yaml:\n" +
                "        lines = [line.rstrip() for line in yaml]\n" +
                "\n" +
                "    for property in yaml_properties:\n" +
                "        key = property.split(\"=\")[0]\n" +
                "        value = property.split(\"=\")[1]\n" +
                "        key = key.replace(\"aerospike.graph-service.\", \"\")\n" +
                "        if key == \"serializers\" or key == \"processors\" or key == \"graphs\":\n" +
                "            # These need to be injected in a weird way and it's unlikely there is a good reason to do anything with these.\n" +
                "            # If a customer has a good reason, we will add support for this later.\n" +
                "            # Graphs requires coordinating the properties file and the yaml so should not be overwritten.\n" +
                "            raise Exception(\"Error configuring Aerospike Graph Service.\\n\\t'serializers', 'processors', and 'graphs' \" + \\\n" +
                "                    \"of gremlin-server config cannot be overwritten by properties file, contact support if you need \" + \\\n" +
                "                    \"to override these configurations.\")\n" +
                "        if key.startswith(\"metrics.\"):\n" +
                "            key = key.replace(\"metrics.\", \"\")\n" +
                "            if key.split(\".\")[0] in metrics:\n" +
                "                metrics_key = key.split(\".\")[0]\n" +
                "                reporter = metrics.get(metrics_key)\n" +
                "                key = key.replace(metrics_key + \".\", \"\")\n" +
                "                if key in reporter:\n" +
                "                    reporter[key] = value\n" +
                "                else:\n" +
                "                    raise Exception(\"Error configuring Aerospike Graph Service.\\n\\t\" + key +\n" +
                "                                    \" is not a valid configuration for metrics of type \" + metrics_key + \".\")\n" +
                "            else:\n" +
                "                raise Exception(\n" +
                "                    \"Error configuring Aerospike Graph Service.\\n\\t\" + key.split(\".\")[0] + \\\n" +
                "                    \" is not a valid metrics type.\")\n" +
                "        else:\n" +
                "            lines = [i for i in lines if not i.startswith(key)]\n" +
                "            rewritten_lines.append(f\"{key}: {value}\")\n" +
                "\n" +
                "    # Metrics\n" +
                "    rewritten_lines.append(\"metrics: { \")\n" +
                "    metrics_count = len(metrics)\n" +
                "    metrics_position = 1\n" +
                "    for reporter_name, reporter in metrics.items():\n" +
                "        rewritten_lines.append(f\"  {reporter_name}:\" + \" { \")\n" +
                "        reporter_count = len(reporter)\n" +
                "        reporter_position = 1\n" +
                "        for setting_name, setting_value in reporter.items():\n" +
                "            if reporter_position == reporter_count:\n" +
                "                rewritten_lines.append(f\"    {setting_name}: {setting_value}\")\n" +
                "            else:\n" +
                "                rewritten_lines.append(f\"    {setting_name}: {setting_value},\")\n" +
                "            reporter_position += 1\n" +
                "        if metrics_position == metrics_count:\n" +
                "            rewritten_lines.append(\"  }\")\n" +
                "        else:\n" +
                "            rewritten_lines.append(\"  },\")\n" +
                "        metrics_position += 1\n" +
                "    rewritten_lines.append(\"}\")\n" +
                "\n" +
                "    rewritten_lines.append(\"graphs: { \")\n" +
                "    for key in graph_config:\n" +
                "        rewritten_lines.append(f\"  {key}: {conf_dir}/aerospike-graph-{key}.properties,\")\n" +
                "    rewritten_lines.append(\"}\")\n" +
                "\n" +
                "    # Pop serializers in here since we can't flatten them.\n" +
                "    rewritten_lines.append(\n" +
                "\"\"\"serializers:\n" +
                "  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3, config: { ioRegistries: [org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3] }}            # application/json\n" +
                "  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1 }                                                                                                           # application/vnd.graphbinary-v1.0\n" +
                "  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1, config: { serializeResultToString: true }}                                                                 # application/vnd.graphbinary-v1.0-stringd\n" +
                "processors:\n" +
                "  - { className: org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor, config: { sessionTimeout: 28800000 }}\n" +
                "  - { className: org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor, config: { cacheExpirationTime: 600000, cacheMaxSize: 1000 }}\n" +
                "\"\"\")\n" +
                "    find_security_credentials(auth_jwt_secret, auth_jwt_issuer, auth_jwt_algorithm, rewritten_lines)\n" +
                "\n" +
                "    lines = lines + rewritten_lines\n" +
                "\n" +
                "    with open(output_yaml_file, \"w\") as yaml:\n" +
                "        for line in lines:\n" +
                "            yaml.write(line + \"\\n\")\n" +
                "\n" +
                "    with open(output_yaml_file, \"r\") as prop:\n" +
                "        output_yaml_str = prop.read()\n" +
                "        output_yaml_print = \"\"\n" +
                "        lines = output_yaml_str.split('\\n')\n" +
                "        for line in lines:\n" +
                "            if \"aerospike.graph-service.auth.jwt.secret\" in line:\n" +
                "                output_yaml_print += \"    aerospike.graph-service.auth.jwt.secret: ********\\n\"\n" +
                "            elif \"aerospike.graph-service.auth.jwt.issuer\" in line:\n" +
                "                output_yaml_print += \"    aerospike.graph-service.auth.jwt.issuer: ********\\n\"\n" +
                "            else:\n" +
                "                output_yaml_print += line + \"\\n\"\n" +
                "        print(\"Generated yaml file: \" + output_yaml_file + \"\\n\" + output_yaml_print)\n" +
                "\n" +
                "\n" +
                "def find_security_credentials(auth_jwt_secret, auth_jwt_issuer, auth_jwt_algorithm, rewritten_lines):\n" +
                "    secret = None\n" +
                "    algorithm = None\n" +
                "    issuer = None\n" +
                "    if auth_jwt_secret is not None:\n" +
                "        secret = auth_jwt_secret.split(\"=\")[1]\n" +
                "    if auth_jwt_issuer is not None:\n" +
                "        issuer = auth_jwt_issuer.split(\"=\")[1]\n" +
                "    if auth_jwt_algorithm is not None:\n" +
                "        algorithm = auth_jwt_algorithm.split(\"=\")[1]\n" +
                "\n" +
                "    if algorithm is not None and secret is None and issuer is None:\n" +
                "        raise Exception(\"Error configuring Aerospike Graph Service.\\n\\t\"\n" +
                "                        \"Configuring security requires both 'aerospike.graph-service.auth.jwt.secret' and \"\n" +
                "                        \"'aerospike.graph-service.auth.jwt.issuer' to be set, but only \"\n" +
                "                        \"'aerospike.graph-service.auth.jwt.algorithm' was set. \"\n" +
                "                        \"('aerospike.graph-service.auth.jwt.secret' and 'aerospike.graph-service.auth.jwt.issuer' are required).\")\n" +
                "    if secret is not None and issuer is None:\n" +
                "        raise Exception(\"Error configuring Aerospike Graph Service.\\n\\t\"\n" +
                "                        \"Configuring security requires both 'aerospike.graph-service.auth.jwt.secret' and \"\n" +
                "                        \"'aerospike.graph-service.auth.jwt.issuer' to be set, but only \"\n" +
                "                        \"'aerospike.graph-service.auth.jwt.secret' was set. \"\n" +
                "                        \"('aerospike.graph-service.auth.jwt.algorithm' is optional).\")\n" +
                "    elif issuer is not None and secret is None:\n" +
                "        raise Exception(\"Error configuring Aerospike Graph Service.\\n\\t\"\n" +
                "                        \"Configuring security requires both 'aerospike.graph-service.auth.jwt.secret' and \"\n" +
                "                        \"'aerospike.graph-service.auth.jwt.issuer' to be set, but only \"\n" +
                "                        \"'aerospike.graph-service.auth.jwt.issuer' was set. \"\n" +
                "                        \"('aerospike.graph-service.auth.jwt.algorithm' is optional).\")\n" +
                "    elif issuer is not None and secret is not None:\n" +
                "        if algorithm is None:\n" +
                "            print(\"Defaulting 'aerospike.graph-service.auth.jwt.algorithm' to 'HMA256'.\")\n" +
                "            algorithm = \"HMAC256\"\n" +
                "        elif algorithm not in [\"HMAC256\", \"HMAC384\", \"HMAC512\"]:\n" +
                "            raise Exception(\"Error configuring Aerospike Graph Service.\\n\\t\"\n" +
                "                            \"Invalid value for 'aerospike.graph-service.auth.jwt.algorithm'. \"\n" +
                "                            \"Valid values are 'HMAC256', 'HMAC384', and 'HMAC512'. \"\n" +
                "                            \"Provided value is '\" + algorithm + \"'.\")\n" +
                "        rewritten_lines.append(\"\"\"authentication: {\n" +
                "  authenticator: com.aerospike.firefly.security.JWTAuthenticator,\n" +
                "  config: {\n" +
                "    aerospike.graph-service.auth.jwt.secret: \"\"\" + secret + \"\"\",\n" +
                "    aerospike.graph-service.auth.jwt.issuer: \"\"\" + issuer)\n" +
                "        if algorithm is not None:\n" +
                "            rewritten_lines.append(\"\"\",\n" +
                "    aerospike.graph-service.auth.jwt.algorithm: \"\"\" + algorithm)\n" +
                "        rewritten_lines.append(\"\"\"\n" +
                "  }\n" +
                "}\n" +
                "authorization: {\n" +
                "    authorizer: com.aerospike.firefly.security.JWTAuthorizer,\n" +
                "    config: {\n" +
                "    }\n" +
                "}\n" +
                "\"\"\")\n" +
                "    else:\n" +
                "        print(\"No security credentials found. Skipping security configuration.\")\n" +
                "\n" +
                "\n" +
                "def generate_properties(properties, output_properties_file, auth_jwt_secret, auth_jwt_issuer):\n" +
                "    with open(output_properties_file, \"w\") as prop:\n" +
                "        if \"gremlin.graph=com.aerospike.firefly.structure.FireflyGraph\" not in properties:\n" +
                "            prop.write(\"gremlin.graph=com.aerospike.firefly.structure.FireflyGraph\\n\")\n" +
                "\n" +
                "        added_properties = []\n" +
                "        for property in reversed(properties):\n" +
                "            prop_name = property[:property.index(\"=\")]\n" +
                "            if not prop_name in added_properties:\n" +
                "                prop.write(property + \"\\n\")\n" +
                "                added_properties.append(prop_name)\n" +
                "\n" +
                "        if auth_jwt_secret is not None and auth_jwt_issuer is not None:\n" +
                "            prop.write(\"aerospike.graph-service.auth.enabled=true\\n\")\n" +
                "\n" +
                "\n" +
                "def generate_java_options(java_options_file_path, max_heap, min_heap):\n" +
                "    java_options = \"\"\n" +
                "\n" +
                "    # We are deprecating JAVA_OPTIONS in favor of using our notation. Users don't need to know we are using Java.\n" +
                "    if max_heap is not None:\n" +
                "        print(\"aerospike.graph-service.heap.max was set to \" + max_heap + \". Using this value for -Xmx.\")\n" +
                "        java_options += f\" -Xmx{max_heap.split('=')[1]} \"\n" +
                "    else:\n" +
                "        try:\n" +
                "            mem_mib = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / (1024. ** 2)\n" +
                "        except Exception as e:\n" +
                "            # to run script on windows\n" +
                "            mem_mib = 1024.\n" +
                "        max_memory = int(mem_mib * 0.8)  # 80% of system memory\n" +
                "        java_options += f\" -Xmx{max_memory}m \"\n" +
                "    if min_heap is not None:\n" +
                "        print(\"aerospike.graph-service.heap.min was set to \" + min_heap + \". Using this value for -Xms.\")\n" +
                "        java_options += f\" -Xms{min_heap.split('=')[1]} \"\n" +
                "\n" +
                "    user_java_options = os.environ.get(\"JAVA_OPTIONS\")\n" +
                "    if user_java_options is not None:\n" +
                "        print(\"Appending user provided JAVA_OPTIONS: \" + user_java_options + \" to java options.\")\n" +
                "        java_options += user_java_options\n" +
                "    java_options += \" --add-exports java.base/sun.nio.ch=ALL-UNNAMED \"\n" +
                "\n" +
                "    # Write classpath to file. Use 'w' to overwrite file.\n" +
                "    with open(java_options_file_path, \"w\") as java_options_file:\n" +
                "        java_options_file.write(java_options)\n" +
                "\n" +
                "\n" +
                "if __name__ == \"__main__\":\n" +
                "    input_properties_file = sys.argv[1]\n" +
                "    default_yaml_file = sys.argv[2]\n" +
                "    output_yaml_file = sys.argv[3]\n" +
                "    output_conf_dir = sys.argv[4]\n" +
                "    output_java_options_file = sys.argv[5]\n" +
                "    print('input_properties_file: ' + input_properties_file)\n" +
                "    print('default_yaml_file: ' + default_yaml_file)\n" +
                "    print('output_yaml_file: ' + output_yaml_file)\n" +
                "    print('output_conf_dir: ' + output_conf_dir)\n" +
                "    print('output_java_options_file: ' + output_java_options_file)\n" +
                "\n" +
                "    try:\n" +
                "        main(input_properties_file, default_yaml_file, output_yaml_file, output_conf_dir, output_java_options_file)\n" +
                "        sys.exit(0)\n" +
                "    except Exception as e:\n" +
                "        print(e)\n" +
                "        sys.exit(1)\n";
    }



    /**
     * Configures the Spark Session to have the right configurations for interacting with different file systems.
     * @param spark Spark session
     * @param cmd   Command line args
     * @param uri   URI to check before reading
     */
    public static void configureFileSystem(final SparkSession spark, final CommandLine cmd, final String uri) {
        final String uriFileSystem = getFileSystem(uri);
        if (fileSystem.equals(uriFileSystem)) {
            // Don't need to do anything if file system did not change.
            return;
        } else if (fileSystem.equals(LOCAL)) {
            LOGGER.info("Remote file system detected. Changing to '" + uriFileSystem + "' mode.");
            fileSystem = uriFileSystem;
            if (fileSystem.equals(S3)) {
                if (cmd.hasOption("u")) {
                    spark.conf().set("fs.s3a.access.key", cmd.getOptionValue("u").trim());
                }
                if (cmd.hasOption("p")) {
                    spark.conf().set("fs.s3a.secret.key", cmd.getOptionValue("p").trim());
                }
            } else if (uriFileSystem.equals(GCS)) {
                if (cmd.hasOption("gck")) {
                    final String keyFilePath = cmd.getOptionValue("gck");
                    LOGGER.info("Google Cloud Service Account key file specified: " + keyFilePath);
                    spark.conf().set("google.cloud.auth.service.account.json.keyfile", keyFilePath);
                } else if (cmd.hasOption("u") && cmd.hasOption("p") && cmd.hasOption("gem")) {
                    LOGGER.info("Google Cloud Service credentials passed in directly.");
                    spark.conf().set("fs.gs.auth.service.account.private.key.id", cmd.getOptionValue("u").trim());
                    spark.conf().set("fs.gs.auth.service.account.private.key", cmd.getOptionValue("p").trim());
                    spark.conf().set("fs.gs.auth.service.account.email", cmd.getOptionValue("gem").trim());
                } else {
                    // Credentials are only necessary in JVM/Local mode.
                    if (cmd.hasOption(LOCAL_MODE)) {
                        final String gcsCredentialError = "Either '" + GCS_KEYFILE_DIRECTORY + "' or all of '" +
                                GCS_EMAIL+ "', '" + REMOTE_USERNAME + "', and '" + REMOTE_PASSKEY +
                                "' must be specified to read from GCS.";
                        LOGGER.error(gcsCredentialError);
                        throw new RuntimeException(gcsCredentialError);
                    }
                }
            }
        } else {
            LOGGER.error("Cannot change file system from '" + fileSystem + "' to '" + uriFileSystem + "'.");
            throw new RuntimeException("Cannot change file system from '" + fileSystem + "' to '" + uriFileSystem + "'.");
        }
    }

    private static String getFileSystem(final String uri) {
        if (uri.toLowerCase().startsWith("s3://")) {
            return S3;
        } else if (uri.toLowerCase().startsWith("gs://")) {
            return GCS;
        } else {
            return LOCAL;
        }
    }

    private static SparkSession buildSparkSession(final CommandLine cmd) {
        SparkConf conf = new SparkConf();
        if (cmd.hasOption(LOCAL_MODE)) {
            conf.setMaster("local[*]");
        }

        conf.setAppName("aerospike-graph-olap")
                .set("spark.driver.allowMultipleContexts", "false")
                .set("spark.ui.enabled", "true")
                .set("mapreduce.fileoutputcommitter.algorithm.version", "2");

        final SparkSession.Builder builder = SparkSession.builder().config(conf);
        builder.config("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                .config("google.cloud.auth.service.account.enable", true);

        // Internal use configurations
        if (cmd.hasOption("s3e")) {
            builder.config("fs.s3a.endpoint", cmd.getOptionValue("s3e")).config("fs.s3a.connection.ssl.enabled", "false");
        }

        return builder.getOrCreate();
    }

    private static Map<String, Object> loadConfiguration(final SparkSession spark, final CommandLine cmd,
                                                  final String configPath) {
        configureFileSystem(spark, cmd, configPath);
        LOGGER.debug("Configuration uri:" + configPath);

        final String fileContext = spark.read().option("wholetext", true).text(configPath).collectAsList().get(0)
                .getString(0);

        final Properties prop = new Properties();
        try (final StringReader reader = new StringReader(fileContext)) {
            prop.load(reader);
        } catch (final IOException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(e);
        }
        final Map<String, Object> config = new MapConfiguration(prop).getMap();
        config.put(ConfigurationHelper.Keys.BULK_LOADER_FLAG.toLowerCase(), "true");
        return config;
    }
}
