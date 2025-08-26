package com.aerospike.firefly.process.call.config;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import java.util.Map;

public class ConfigServiceDumpDefaults<I, R> extends ConfigServiceBase<I, R> {

    public ConfigServiceDumpDefaults(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminServiceName() {
        return "dump-defaults";
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
        return (R) ConfigurationHelper.dumpDefaultsMap();
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("[{}] - {} - Get Config Defaults.", getUser(), getName());
    }
}
