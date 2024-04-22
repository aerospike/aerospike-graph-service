package com.aerospike.firefly.process.call.sindex;

import com.aerospike.firefly.io.aerospike.admin.Admin;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.HashMap;
import java.util.Map;

/*
    g.call("aerospike.graph.admin.index.status").
      with("element_type", "<element_type>").
      with("property_key", "<property_key>"); -> status
 */
public class SindexServiceStatus<I, R> extends SindexServiceBase<I, R> {

    public SindexServiceStatus(final FireflyGraph firefly) {
        super(firefly);
    }

    @Override
    protected String getAdminServiceName() {
        return "status";
    }

    @Override
    public Map<String, String> describeParams() {
        // No parameters.
        final Map<String, String> parameters = new HashMap<>();
        parameters.put(ELEMENT_TYPE, "The type of element to get the index status of. Only 'vertex' is currently supported.");
        parameters.put(PROPERTY_KEY, "The property key to get the index status of. '~label' can be used to get the status of an index on labels.");
        return parameters;
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to %s.\n" +
                        "\tExpected '" + ELEMENT_TYPE + "' and '" + PROPERTY_KEY + "' parameters provided.\n" +
                        "\tNote, only 'vertex' is currently supported for '" + ELEMENT_TYPE + "'.\n" +
                        "\tProvided arguments: %s.\n" +
                        "\tExamples of correct usage:\n" +
                        "\t\tg.call(\"%s\").with(\"" + ELEMENT_TYPE + "\", \"vertex\").with(\"" + PROPERTY_KEY + "\", \"~label\").next();\n" +
                        "\t\tg.call(\"%s\").with(\"" + ELEMENT_TYPE + "\", \"vertex\").with(\"" + PROPERTY_KEY + "\", \"name\").next();",
                getName(), params, getName(), getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        // Should be no parameters.
        if (params.size() != 2 || !params.containsKey(ELEMENT_TYPE) || !params.containsKey(PROPERTY_KEY)) {
            return false;
        } else if (!params.get(ELEMENT_TYPE).equals("vertex")) {
            return false;
        } else if (!(params.get(PROPERTY_KEY) instanceof String)) {
            return false;
        }
        return true;
    }

    @Override
    protected R execute(final Map params) {
        if (params.get(ELEMENT_TYPE).equals("vertex")) {
            if (params.get(PROPERTY_KEY).equals("~label")) {
                return (R) Admin.index.getStatusVertexLabelIndex(graph);
            } else {
                return (R) Admin.index.getStatusVertexPropertyIndex(graph, (String) params.get(PROPERTY_KEY));
            }
        } else {
            // Should be caught by sanitize().
            throw new IllegalArgumentException("Only 'vertex' is currently supported for '" + ELEMENT_TYPE + "'.");
        }
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info(getName() + " Get status.");
    }

    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.READ;
    }
}
