package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.util.Exceptions;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyHelper {
    public static boolean inComputerMode(final FireflyGraph graph) {
        return false;
    }

    protected static Edge addEdge(final FireflyGraph graph, final FireflyVertex outVertex, final FireflyVertex inVertex, final String label, final Object... keyValues) {
        throw new Exceptions.Unimplemented();
    }

    public static Object getEdges(FireflyVertex fireflyVertex, Direction direction, String[] edgeLabels) {
        throw new Exceptions.Unimplemented();
    }

    public static Object getVertices(FireflyVertex fireflyVertex, Direction direction, String[] edgeLabels) {
        AerospikeConnection db = ((FireflyGraph) fireflyVertex.graph()).db;
        return null;
    }
}
