package com.aerospike.firefly.process.call.sindex;

import com.aerospike.firefly.io.aerospike.admin.Admin;
import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/*
    g.call("aerospike.graph.admin.index.drop").
      with("element_type", "<element_type>").
      with("property_key", "<property_key>"); -> drop
 */
public class SindexServiceDrop<I, R> extends SindexServiceBase<I, R> {
    private static final Logger LOG = LoggerFactory.getLogger(SindexServiceDrop.class);

    public SindexServiceDrop(final FireflyGraph firefly) {
        super(firefly);
    }

    @Override
    protected String adminServiceName() {
        return "drop";
    }

    @Override
    protected Map<String, String> getParamDescription() {
        // No parameters.
        final Map<String, String> parameters = new HashMap<>();
        parameters.put(ELEMENT_TYPE, "The type of element to drop the index on. Only 'vertex' is currently supported.");
        parameters.put(PROPERTY_KEY, "The property key to drop the index on. '~label' can be used to drop an index on labels.");
        return parameters;
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to %s.\n" +
                        "\tExpected '" + ELEMENT_TYPE + "' and '" + PROPERTY_KEY + "' parameters provided.\n" +
                        "\tNote, only 'vertex' is currently supported for '" + ELEMENT_TYPE + "'.\n" +
                        "\tProvided arguments: %s.\n" +
                        "\tExamples of correct usage:\n" +
                        "\t\tg.call(\"aerospike.graph.admin.index.drop\").with(\"" + ELEMENT_TYPE + "\", \"vertex\").with(\"" + PROPERTY_KEY + "\", \"~label\").next();\n" +
                        "\t\tg.call(\"aerospike.graph.admin.index.drop\").with(\"" + ELEMENT_TYPE + "\", \"vertex\").with(\"" + PROPERTY_KEY + "\", \"name\").next();",
                getName(), params);
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
            try {
                if (params.get(PROPERTY_KEY).equals("~label")) {
                    return (R) Admin.index.dropVertexLabelIndex(firefly, new EmptyAdminContext());
                } else {
                    return (R) Admin.index.dropVertexPropertyIndex(firefly, (String) params.get(PROPERTY_KEY), new EmptyAdminContext());
                }
            } finally {
                try {
                    firefly.fireflyIndexMetadata.updateMetadata();
                } catch (final Exception e) {
                    LOG.warn("Updating Index metadata forcibly due to dropping an index failed.", e);
                }
            }
        } else {
            // Should be caught by sanitize().
            throw new IllegalArgumentException("Only 'vertex' is currently supported for '" + ELEMENT_TYPE + "'.");
        }
    }
}
