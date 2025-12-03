package com.aerospike.firefly.process.call.metadata;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MetadataServiceConfig<I, R> extends MetadataServiceBase<I, R> {

    public static final String GREMLIN_SERVER_CONFIG = "Gremlin Server Configuration";
    public static final String GRAPH_PROPERTIES = "Graph Properties";
    private static final String KEY = "mode";
    private static final String MODE_FULL = "full";
    private static final String MODE_DELTA = "delta";


    public MetadataServiceConfig(final FireflyGraph graph) {
        super(graph);
    }

    public static boolean isSensitive(final String key) {
        final String lowerCaseKey = key.toLowerCase();
        return lowerCaseKey.contains("password") || lowerCaseKey.contains("secret") || lowerCaseKey.contains("token") || lowerCaseKey.contains("passkey");
    }

    private static String serializeFile(final String file) {
        final StringBuilder sb = new StringBuilder();
        try (final BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line = br.readLine();
            while (line != null) {
                if (sb.length() != 0) {
                    sb.append("\n");
                }
                sb.append(line);
                line = br.readLine();
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return sb.toString();
    }

    @Override
    protected String getAdminServiceName() {
        return "config";
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to '%s'.\n" +
                        "\tExpected no arguments or argument key '%s' with value of '%s' or '%s'.\n" +
                        "\tProvided argument: '%s'.\n" +
                        "\tExamples of correct usage:\n" +
                        "\t\tg.call(\"%s\").next();\n" +
                        "\t\tg.call(\"%s\").with(\"%s\", \"%s\").next();\n" +
                        "\t\t or \n" +
                        "\t\tg.call(\"%s\").with(\"%s\", \"%s\").next();\n",
                getName(), KEY, MODE_FULL, MODE_DELTA, params, getName(),
                getName(), KEY, MODE_DELTA, getName(), KEY, MODE_FULL);
    }

    @Override
    protected boolean sanitize(final Map params) {
        if (params.isEmpty()) {
            return true;
        } else if (params.size() != 1) {
            return false;
        } else if (!params.containsKey(KEY)) {
            return false;
        } else {
            return params.get(KEY).equals(MODE_FULL) || params.get(KEY).equals(MODE_DELTA);
        }
    }

    @Override
    protected R execute(final Map params) {
        final Map<String, Object> completeConfig = new HashMap<>();
        final Configuration configuration = graph.getBaseGraph().getConfig().getRawConfig();
        final Map<String, Object> configurationMap = new HashMap<>();

        if (!params.isEmpty() && params.containsValue(MODE_FULL)) {
            final Map<Object, String> defaults = ConfigurationHelper.getDefaultConfigMap();
            for (final Map.Entry<Object, String> entry : defaults.entrySet()) {
                final String key = entry.getKey().toString();
                final Object value;

                if (isSensitive(key)) {
                    value = "*******";
                } else if (configuration.containsKey(key)) {
                    value = configuration.getString(key);
                } else {
                    value = entry.getValue();
                }

                configurationMap.put(key, value);
            }
        } else {
            final Iterator<String> keys = configuration.getKeys();
            while (keys.hasNext()) {
                final String key = keys.next();
                if (isSensitive(key)) {
                    configurationMap.put(key, "********");
                } else {
                    configurationMap.put(key, configuration.getProperty(key));
                }
            }
        }
        completeConfig.put(GRAPH_PROPERTIES, configurationMap);

        try {
            final String gremlinServerFile = FireflyGraph.getGremlinServerYamlFile();
            completeConfig.put(GREMLIN_SERVER_CONFIG, serializeFile(gremlinServerFile));
        } catch (final Exception e) {
            LOGGER.error("Could not read gremlin-server yaml file: {}", e.getMessage());
            completeConfig.put(GREMLIN_SERVER_CONFIG, "Not available");
        }

        return (R) completeConfig;
    }

    @Override
    public Map<String, String> describeParams() {
        return Map.of();
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("[{}] - {} - Get graph configuration.", getUser(), getName());
    }
}
