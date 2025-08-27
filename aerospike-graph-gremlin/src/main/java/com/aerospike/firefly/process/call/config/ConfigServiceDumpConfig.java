package com.aerospike.firefly.process.call.config;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.configuration2.Configuration;

public class ConfigServiceDumpConfig<I, R> extends ConfigServiceBase<I, R> {

    public ConfigServiceDumpConfig(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminServiceName() {
        return "dump-config";
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to '%s'.\n" +
                "\tExpected no arguments.\n" +
                "\tProvided arguments: '%s'.\n" +
                "\tExample of correct usage:\n" +
                "\t\tg.call(\"%s\").next();\n",
                getName(), params, getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        return params.isEmpty();
    }

    @Override
    protected R execute(final Map params) {
        Configuration config = graph.configuration();
        Map<Object, String> defaults = ConfigurationHelper.dumpDefaultConfigMap();
        Map<String, Object> result = new HashMap<>();
        for (final Object key : defaults.keySet()) {
            String keyStr = key.toString();
            Object value;
            if (keyStr.contains("password") || keyStr.contains("secret") || keyStr.contains("token") || keyStr.contains("passkey")) {
                value = "*******";
            } else {
                value = ConfigurationHelper.getOrDefault(keyStr, config);
            }
            result.put(keyStr, value);
        }

        return (R) result;
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("[{}] - {} - Get Config Values.", getUser(), getName());
    }
}
