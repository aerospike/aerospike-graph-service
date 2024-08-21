package com.aerospike.firefly.process.call.sindex;

import com.aerospike.firefly.io.aerospike.admin.Admin;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Collections;
import java.util.Map;

// Used as g.call("aerospike.graph.admin.index.list").next();
public class SindexServiceList<I, R> extends SindexServiceBase<I, R> {

    public SindexServiceList(final FireflyGraph firefly) {
        super(firefly);
    }

    @Override
    protected String getAdminServiceName() {
        return "list";
    }

    @Override
    public Map<String, String> describeParams() {
        // No parameters.
        return Collections.emptyMap();
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to %s.\n" +
                        "\tExpected no arguments provided.\n" +
                        "\tProvided arguments: %s.\n" +
                        "\tExample of correct usage: g.call(\"%s\").next();",
                getName(), params, getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        // Should be no parameters.
        return params.isEmpty();
    }

    @Override
    protected R execute(final Map params) {
        return (R) Admin.index.getIndexList(graph);
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("{" + getUser() + "} - " + getName() + " - List indexes.");
    }

    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.READ;
    }
}
