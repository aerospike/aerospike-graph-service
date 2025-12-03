package com.aerospike.firefly.process.call.metadata;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.util.Gremlin;

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
                getName(), params, getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        return params.keySet().isEmpty();
    }

    @Override
    protected R execute(final Map params) {
        return (R) Map.of(
                "Aerospike version", AerospikeConnection.InfoOps.getDatabaseVersionString(graph.getBaseGraph()),
                "Aerospike Graph Service version", graph.FIREFLY_VERSION,
                "Gremlin version", Gremlin.version());
    }

    @Override
    public Map<String, String> describeParams() {
        return Map.of();
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("[{}] - {} - Get Aerospike version and Aerospike Graph Service version.", getUser(), getName());
    }
}
