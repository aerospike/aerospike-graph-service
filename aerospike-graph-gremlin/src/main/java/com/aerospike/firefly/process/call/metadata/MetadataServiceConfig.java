package com.aerospike.firefly.process.call.metadata;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MetadataServiceConfig<I, R> extends MetadataServiceBase<I, R> {


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
        // TODO: Gremlin-server yaml?
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
        return (R) configurationMap;
    }

    @Override
    public Map<String, String> describeParams() {
        return Map.of();
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info(getName() + " Get graph configuration.");
    }
}
