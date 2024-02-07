package com.aerospike.firefly.process.call.sindex;

import com.aerospike.firefly.io.aerospike.admin.Admin;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Collections;
import java.util.Map;

// Used as g.call("aerospike.graph.admin.index.cardinality").next();
public class SindexServiceCardinality<I, R> extends SindexServiceBase<I, R> {

    public SindexServiceCardinality(final FireflyGraph firefly) {
        super(firefly);
    }

    @Override
    protected String adminServiceName() {
        return "cardinality";
    }

    @Override
    protected Map<String, String> getParamDescription() {
        // No parameters.
        return Collections.emptyMap();
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to %s.\n" +
                        "\tExpected no arguments provided.\n" +
                        "\tProvided arguments: %s.\n" +
                        "\tExample of correct usage: g.call(\"aerospike.graph.admin.index.cardinality\").next();",
                getName(), params);
    }

    @Override
    protected boolean sanitize(final Map params) {
        // Should be no parameters.
        return params.isEmpty();
    }

    @Override
    protected R execute(final Map params) {
        return (R) Admin.index.getIndexCardinality(firefly, new EmptyAdminContext());
    }
}
