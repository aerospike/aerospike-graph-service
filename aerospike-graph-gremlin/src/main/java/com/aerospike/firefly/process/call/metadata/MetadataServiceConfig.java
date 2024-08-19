package com.aerospike.firefly.process.call.metadata;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MetadataServiceConfig<I, R> extends MetadataServiceBase<I, R> {

    public static final String GREMLIN_SERVER_CONFIG = "Gremlin Server Configuration";
    public static final String UNIFIED_CONFIG = "Unified Configuration";
    public static final String GRAPH_PROPERTIES = "Graph Properties";


    public MetadataServiceConfig(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminServiceName() {
        return "config";
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to '%s'.\n" +
                        "\tExpected no arguments.\n" +
                        "\tProvided arguments: '%s'.\n" +
                        "\tExample of correct usage:\n" +
                        "\t\tg.call(\"%s\").next();\n",
                getName(), params);
    }

    @Override
    protected boolean sanitize(final Map params) {
        return params.keySet().isEmpty();
    }

    @Override
    protected R execute(final Map params) {
        final Map<String, Object> completeConfig = new HashMap<>();
        final Configuration configuration = graph.configuration();
        final Iterator<String> keys = configuration.getKeys();
        final Map<String, Object> configurationMap = new HashMap<>();
        while (keys.hasNext()) {
            final String key = keys.next();
            if (!key.contains("password") && !key.contains("secret") && !key.contains("token")) {
                configurationMap.put(key, configuration.getProperty(key));
            } else {
                configurationMap.put(key, "********");
            }
        }
        completeConfig.put(GRAPH_PROPERTIES, configurationMap);

        try {
            final String gremlinServerFile = FireflyGraph.getGremlinServerYamlFile();
            completeConfig.put(GREMLIN_SERVER_CONFIG, serializeFile(gremlinServerFile));
        } catch (final Exception e) {
            LOGGER.error("Could not read gremlin-server yaml file: " + e.getMessage());
            completeConfig.put(GREMLIN_SERVER_CONFIG, "Not available");
        }

        try {
            final String unifiedConfig = FireflyGraph.getUnifiedConfigFile();
            completeConfig.put(UNIFIED_CONFIG, serializeFile(unifiedConfig));
        } catch (final Exception e) {
            LOGGER.error("Could not read unified config file: " + e.getMessage());
            completeConfig.put(UNIFIED_CONFIG, "Not available");
        }
        return (R) completeConfig;
    }

    private static String serializeFile(final String file) {
        final StringBuilder sb = new StringBuilder();
        try (final BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (sb.length() != 0) {
                    sb.append("\n");
                }
                sb.append(line);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return sb.toString();
    }

    @Override
    public Map<String, String> describeParams() {
        return Map.of();
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("{" + getUser() + "} - " + getName() + " - Get graph configuration.");
    }
}
