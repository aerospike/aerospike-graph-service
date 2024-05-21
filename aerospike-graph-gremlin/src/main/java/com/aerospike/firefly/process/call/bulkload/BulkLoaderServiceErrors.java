package com.aerospike.firefly.process.call.bulkload;

import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Map;
import java.util.Set;

public class BulkLoaderServiceErrors<I, R> extends BulkLoaderServiceBase<I, R> {
    private static final String KEY = "type";
    private static final String DUPLICATE_VID = "duplicate-vertex-ids";
    private static final String BAD_ENTRY = "bad-entries";
    private static final String BAD_EDGE = "bad-edges";
    private static final Set<String> VALUES = Set.of(DUPLICATE_VID, BAD_ENTRY, BAD_EDGE);

    public BulkLoaderServiceErrors(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminServiceName() {
        return "errors";
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to '%s'.\n" +
                "\tExpected argument key '%s' with value of any of '%s'.\n" +
                "\tProvided argument: '%s'.\n" +
                "\tExamples of correct usage:\n" +
                "\t\tg.call(\"%s\").with(\"%s\").next();\n" +
                "\t\tg.call(\"%s\").with(\"%s\").next();\n" +
                "\t\tg.call(\"%s\").with(\"%s\").next();\n",
                getName(), KEY, VALUES, params,
                getName(), KEY, BAD_ENTRY,
                getName(), KEY, DUPLICATE_VID,
                getName(), KEY, BAD_EDGE);
    }

    @Override
    protected boolean sanitize(final Map params) {
        return (params.size() == 1 && params.containsKey(KEY) && VALUES.contains(params.get(KEY)));
    }

    @Override
    protected R execute(final Map params) {
        if (params.get(KEY).equals(DUPLICATE_VID)) {
            return (R) graph.readDuplicateVertexIdErrors();
        } else if (params.get(KEY).equals(BAD_ENTRY)) {
            return (R) graph.readBadEntryErrors();
        } else if (params.get(KEY).equals(BAD_EDGE)) {
            return (R) graph.readBadEdgeErrors();
        } else {
            // This should never happen since it's already safety checked via sanitization.
            throw new IllegalStateException(
                    "Invalid \"" + KEY + "\" detected for \"aerospike.graphloader.admin.bulk-load.errors\": '" + params.get(KEY) + "'.");
        }
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info(getName() + " Get bulk load errors.");
    }
}
