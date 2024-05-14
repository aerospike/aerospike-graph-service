package com.aerospike.firefly.process.call.metadata;

import com.aerospike.firefly.io.aerospike.FireflyAerospikeVersionCheck;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Map;

public class MetadataServiceVersion<I, R> extends MetadataServiceBase<I, R> {


    public MetadataServiceVersion(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminServiceName() {
        return "version";
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
        return (R) Map.of(
                "Aerospike version", FireflyAerospikeVersionCheck.getVersionString(graph.getBaseGraph().client),
                "Aerospike Graph Service version", graph.FIREFLY_VERSION);
    }

    @Override
    public Map<String, String> describeParams() {
        return Map.of();
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info(getName() + " Get Aerospike version and Aerospike Graph Service version.");
    }
}
