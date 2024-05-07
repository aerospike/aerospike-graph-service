package com.aerospike.firefly.process.computer.util;

import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GraphFilterHelper {

    private GraphFilterHelper() {

    }

    public static boolean filterAllEdges(final GraphFilter filter) {
        return filter.hasEdgeFilter() && filter.getEdgeFilter().getEndStep() instanceof RangeGlobalStep && ((RangeGlobalStep) filter.getEdgeFilter().getEndStep()).getHighRange() == 0L;
    }
}
