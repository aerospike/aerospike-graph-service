package com.aerospike.firefly.io.impl.relational.packed;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.relational.RelationalVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.List;
import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class PackedVertex extends RelationalVertex {

    /**
     * Constructor for PackedVertex.
     *
     * @param fid                           firefly id.
     * @param label                         label.
     * @param graph                         graph.
     * @param inEdgeIds                     incoming edge ids - null if invalid (cache disabled or too many).
     * @param outEdgeIds                    outgoing edge ids - null if invalid (cache disabled or too many).
     * @param inEdgeCount                   incoming edge count.
     * @param outEdgeCount                  outgoing edge count.
     * @param vertexPropertyIds             vertex property ids.
     * @param vertexPropertyValues          vertex property values.
     * @param vertexPropertyValuesTypeHints vertex property value type hints.
     * @param isEdgeCacheOverflowed         is the edge cache overflowed.
     * @param db                            Aerospike connection.
     */
    protected PackedVertex(final FireflyId fid,
                           final String label,
                           final FireflyGraph graph,
                           final Map<String, List<FireflyId>> inEdgeIds,
                           final Map<String, List<FireflyId>> outEdgeIds,
                           final long inEdgeCount,
                           final long outEdgeCount,
                           final Map<String, FireflyId> vertexPropertyIds,
                           final Map<String, Object> vertexPropertyValues,
                           final Map<String, Object> vertexPropertyValuesTypeHints,
                           final Map<Object, Map<String, Object>> vertexPropertyIdToProperties,
                           final Map<Object, Map<String, Object>> vertexPropertyIdToTypeHints,
                           final boolean isEdgeCacheOverflowed,
                           final AerospikeConnection db) {
        super(fid, label, graph, inEdgeIds, outEdgeIds, inEdgeCount, outEdgeCount, isEdgeCacheOverflowed, db, vertexPropertyIds, vertexPropertyValues, vertexPropertyValuesTypeHints, vertexPropertyIdToProperties, vertexPropertyIdToTypeHints);

        // To enable values to have index functions run, cardinality must be single.
        if (graph().features().vertex().getCardinality("") != VertexProperty.Cardinality.single) {
            throw new RuntimeException("PackedVertex only supports for single cardinality");
        }

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
