package com.aerospike.firefly.process.call.sindex;

import com.aerospike.firefly.io.aerospike.admin.Admin;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.HashMap;
import java.util.Map;

/*
    g.call("aerospike.graph.admin.index.create").
      with("element_type", "<element_type>").
      with("property_key", "<property_key>"); -> create
 */
public class SindexServiceCreate<I, R> extends SindexServiceBase<I, R> {

    public SindexServiceCreate(final FireflyGraph firefly) {
        super(firefly);
    }

    @Override
    protected String getAdminServiceName() {
        return "create";
    }

    @Override
    public Map<String, String> describeParams() {
        // No parameters.
        final Map<String, String> parameters = new HashMap<>();
        parameters.put(ELEMENT_TYPE, "The type of element to create the index on. Only 'vertex' is currently supported.");
        parameters.put(PROPERTY_KEY, "The property key to create the index on. '~label' can be used to create an index on labels.");
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
                return (R) Admin.index.createVertexLabelIndex(graph);
            } else {
                return (R) Admin.index.createVertexPropertyIndex(graph, (String) params.get(PROPERTY_KEY));
            }
        } else {
            // Should be caught by sanitize().
            throw new IllegalArgumentException("Only 'vertex' is currently supported for '" + ELEMENT_TYPE + "'.");
        }
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info(getName() + " Create sindex.");
    }
}
