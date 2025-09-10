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
    public static final String KEY = "mode";
    public static final String DEFAULTS = "defaults";

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
                        "\tExpected no arguments or argument key '%s' with value of '%s'.\n" +
                        "\tProvided argument: '%s'.\n" +
                        "\tExamples of correct usage:\n" +
                        "\t\tg.call(\"%s\").with(\"%s\", \"%s\").next();\n" +
                getName(), KEY, DEFAULTS, params,
                getName(), KEY, DEFAULTS);
    }

    @Override
    protected boolean sanitize(final Map params) {
        return params.isEmpty() || (params.size() == 1 && params.containsKey(KEY) && params.get(KEY) == DEFAULTS);
    }

    @Override
    protected R execute(final Map params) {
        final Map<String, Object> completeConfig = new HashMap<>();
        final Configuration configuration = graph.configuration();
        final Map<String, Object> configurationMap = new HashMap<>();

        if(params.containsKey(KEY) && params.get(KEY) == DEFAULTS) {
            Map<Object, String> defaults = ConfigurationHelper.getDefaultConfigMap();
            for (final Map.Entry<Object, String> entry : defaults.entrySet()) {
                String key = entry.getKey().toString();
                Object value = entry.getValue();

                if (configuration.containsKey(key)) {
                    value = configuration.getString(key);
                }
                if (isSensitive(key)) {
                    value = "*******";
                }
                configurationMap.put(key, value);
            }
        }else{
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
            LOGGER.error("Could not read gremlin-server yaml file: " + e.getMessage());
            completeConfig.put(GREMLIN_SERVER_CONFIG, "Not available");
        }

        return (R) completeConfig;
    }

    private static boolean isSensitive(final String key) {
        return key.contains("password") || key.contains("secret") || key.contains("token");
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
        LOGGER.info("[{}] - {} - Get graph configuration.", getUser(), getName());
    }
}
