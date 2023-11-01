package com.aerospike.firefly.io.impl.relational;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;

import java.util.List;
import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public abstract class RelationalVertex extends FireflyVertex {

    /**
     * Constructor for RelationalVertex.
     *
     * @param fid                   firefly id.
     * @param label                 label.
     * @param graph                 graph.
     * @param inEdgeIds             incoming edge ids - null if invalid (cache disabled or too many).
     * @param outEdgeIds            outgoing edge ids - null if invalid (cache disabled or too many).
     * @param inEdgeCount           incoming edge count.
     * @param outEdgeCount          outgoing edge count.
     * @param isEdgeCacheOverflowed is the edge cache overflowed.
     * @param db                    Aerospike connection.
     */
    protected RelationalVertex(final FireflyId fid,
                               final String label,
                               final FireflyGraph graph,
                               final Map<String, List<FireflyId>> inEdgeIds,
                               final Map<String, List<FireflyId>> outEdgeIds,
                               final long inEdgeCount,
                               final long outEdgeCount,
                               final boolean isEdgeCacheOverflowed,
                               final AerospikeConnection db, final Map<String, FireflyId> vertexPropertyIds, final Map<String, Object> vertexPropertyValues, final Map<String, Object> vertexPropertyValuesTypeHints, final Map<Object, Map<String, Object>> vertexPropertyIdToProperties, final Map<Object, Map<String, Object>> vertexPropertyIdToTypeHints) {
        super(fid, label, graph, inEdgeIds, outEdgeIds, vertexPropertyIds, vertexPropertyValues, vertexPropertyValuesTypeHints, vertexPropertyIdToProperties, vertexPropertyIdToTypeHints, inEdgeCount, outEdgeCount, isEdgeCacheOverflowed, db);
    }


    public static class PackedVertexFactory {
        public static PackedVertex create(final FireflyId fid,
                                          final String label,
                                          final FireflyGraph graph,
                                          final Map<String, List<FireflyId>> inEdgeIds,
                                          final Map<String, List<FireflyId>> outEdgeIds,
                                          final long inEdgeCount,
                                          final long outEdgeCount,
                                          final Map<String, FireflyId> vertexPropertyIds,
                                          final Map<String, Object> vertexPropertyValues,
                                          final Map<String, Object> vertexPropertyValuesTypeHints,
                                          final Map<Object, Map<String, Object>> vertexPropertyProperties,
                                          final Map<Object, Map<String, Object>> vertexPropertyPropertiesTypeHints,
                                          final boolean isEdgeCacheOverflowed,
                                          final AerospikeConnection db) {
            return new PackedVertex(fid, label, graph, inEdgeIds, outEdgeIds, inEdgeCount, outEdgeCount,
                    vertexPropertyIds, vertexPropertyValues, vertexPropertyValuesTypeHints,
                    vertexPropertyProperties, vertexPropertyPropertiesTypeHints, isEdgeCacheOverflowed, db);
        }
    }
}
