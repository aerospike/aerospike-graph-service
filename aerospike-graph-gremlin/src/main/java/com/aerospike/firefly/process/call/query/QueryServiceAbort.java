package com.aerospike.firefly.process.call.query;

import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Map;

public class QueryServiceAbort<I, R> extends QueryServiceBase<I, R> {

    public QueryServiceAbort(final FireflyGraph firefly) {
        super(firefly);
    }

    @Override
    protected String getAdminServiceName() {
        return "abort";
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to %s.\n" +
                        "\tExpected no arguments.\n" +
                        "\tProvided arguments: %s.\n" +
                        "\tExamples of correct usage:\n" +
                        "\t\tg.call(\"%s\").next();",
                getName(), params, getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        return params.isEmpty();
    }

    @Override
    protected R execute(final Map params) {
        return (R) this.graph.getBaseGraph().abortQueries();
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("{} Cancelling all ongoing queries.", getName());
    }
}
