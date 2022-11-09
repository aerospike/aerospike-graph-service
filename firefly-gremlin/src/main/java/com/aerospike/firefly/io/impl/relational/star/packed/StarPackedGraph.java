package com.aerospike.firefly.io.impl.relational.star.packed;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.packed.PackedGraph;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertex;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertexProperty;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class StarPackedGraph extends PackedGraph {
    public static final String DATA_MODEL = "starpacked";
    private static final Logger LOG = LoggerFactory.getLogger(StarPackedGraph.class);
    private static final String ENABLE_OUT_VP = "out_vp";
    private static final String ENABLE_IN_VP = "in_vp";
    private static final String ENABLE_OUT_OUT = "out_out";
    private static final String ENABLE_OUT_IN = "out_in";
    private static final String ENABLE_IN_OUT = "in_out";
    private static final String ENABLE_IN_IN = "in_in";
    public final boolean enableOutVp;
    public final boolean enableInVp;
    public final boolean enableOutOut;
    public final boolean enableOutIn;
    public final boolean enableInOut;
    public final boolean enableInIn;

    /**
     * Constructor for StarLinkedGraph.
     *
     * @param db   AerospikeConnection.
     * @param conf Configuration.
     */
    public StarPackedGraph(final AerospikeConnection db, final Configuration conf) {
        super(db, conf);
        TraversalStrategies.GlobalCache.registerStrategies(
                StarPackedGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(FireflyGraph.class).clone());
        List<String> foo = new ArrayList<>();
        foo.add("bazz");
        enableOutVp = db.OPTIMIZED_HOP_CONSTRAINT_STEPS.contains(ENABLE_OUT_VP);
        enableInVp = db.OPTIMIZED_HOP_CONSTRAINT_STEPS.contains(ENABLE_IN_VP);
        enableOutOut = db.OPTIMIZED_TWO_HOP_STEPS.contains(ENABLE_OUT_OUT);
        enableOutIn = db.OPTIMIZED_TWO_HOP_STEPS.contains(ENABLE_OUT_IN);
        enableInOut = db.OPTIMIZED_TWO_HOP_STEPS.contains(ENABLE_IN_OUT);
        enableInIn = db.OPTIMIZED_TWO_HOP_STEPS.contains(ENABLE_IN_IN);

        LOG.info("StarPackedGraph: enableOutVp={}, enableInVp={}, enableOutOut={}, enableOutIn={}, enableInOut={}, enableInIn={}",
                enableOutVp, enableInVp, enableOutOut, enableOutIn, enableInOut, enableInIn);
        LOG.info("StarPackedGraph: db.OPTIMIZED_HOP_CONSTRAINT_STEPS={}", db.OPTIMIZED_HOP_CONSTRAINT_STEPS);
        LOG.info("StarPackedGraph: db.OPTIMIZED_TWO_HOP_STEPS={}", db.OPTIMIZED_TWO_HOP_STEPS);
    }

    public static void removeVertexProperty(final AerospikeConnection db, final FireflyVertex vertex, final String key) {
        // Remove vertex property from adjacent vertex in/out property maps.
        StarPackedGraph graph = ((StarPackedGraph)vertex.graph());
        StarPackedVertex.removeVertexPropertyFromAdjacentVertices(db, vertex, key, graph.enableInVp, graph.enableOutVp);
    }

    public static void removeEdge(final AerospikeConnection db, final FireflyEdge edge) {
        // Concrete example of how to remove an edge between two vertices:
        //
        // First let's consider the following sets:
        //   Vertex, inVP, outVP, inIn, inOut, outIn, outOut.
        //      Vertex holds the data 1 one vertex.
        //      inVP holds the properties for the attached in vertices.
        //      outVP holds the properties for the attached out vertices.
        //      inIn holds the in edges for the in vertices.
        //      inOut holds the out edges for the in vertices.
        //      outIn holds the in edges for the out vertices.
        //      outOut holds the out edges for the out vertices.
        //
        // To remove an edge from the inV and outV, we must:
        //  1. Find the properties of the out vertex in the outVP set of the in vertex and remove it.
        //  2. Find the properties of the in vertex in the inVP set of the out vertex and remove it.
        //  3. Loop through the inVertex in and out edges and remove out.in and in.in paths to outVertex via inVertex from adjacent vertices.
        //  4. Loop through the outVertex in and out edges and remove an out.out and in.out paths to inVertex via outVertex from adjacent vertices.
        //  5. Remove in.in and in.out paths that go through outVertex from inVertex.
        //  6. Remove out.in and out.out paths that go through inVertex from outVertex.
        //  Actual edge is removed separately, and so is the edge in the vertex records.

        // 1. Find the properties of the out vertex in the outVP set of the in vertex and remove it.
        StarPackedGraph graph = ((StarPackedGraph)edge.graph());
        if (graph.enableOutVp) {
            StarPackedVertex.removeAdjacentVertexPropertiesFromVertex(db, edge, Direction.OUT);
        }

        // 2. Find the properties of the in vertex in the inVP set of the out vertex and remove it.
        if (graph.enableInVp) {
            StarPackedVertex.removeAdjacentVertexPropertiesFromVertex(db, edge, Direction.IN);
        }

        // 3/4. Loop through the inVertex/outVertex in and out edges and remove appropriate in.in/out.in and in.out/out.out paths to adjacent vertices.
        StarPackedVertex.removeCompoundEdgesFromAdjacentVertices(db, edge, graph.enableOutOut, graph.enableOutIn, graph.enableInOut, graph.enableInIn);

        // 5/6. Remove in.in and in.out paths that go through outVertex from inVertex and out.in and out.out paths that go through inVertex from outVertex.
        StarPackedVertex.removeCompoundEdges(db, edge, graph.enableOutOut, graph.enableOutIn, graph.enableInOut, graph.enableInIn);
    }

    public static void removeVertex(final AerospikeConnection db, final FireflyVertex vertex) {
        // To remove the vertex, we must simply remove the additional sets that are associated with the vertex.
        // The edge addition / removal is what builds and tears down the additional data in the sets,
        // so we don't need any fancy logic for that here.

        // Create list of all relevant sets, would be nice to make this static but unfortunately there are runtime additions.
        final List<String> sets = List.of(db.IN_IN_SET, db.IN_OUT_SET, db.OUT_IN_SET, db.OUT_OUT_SET, db.IN_VP_SET, db.OUT_VP_SET);

        // Remove vertex from the list.
        LOG.debug("Removing vertex {} from sets: {}.", vertex.id, sets);
        sets.forEach(set -> db.delete(FireflyRecord.getKey(db.getNamespace(), db.VERTEX_AERO_SET, vertex.id)));
    }

    public static boolean isStarPackedGraph(final FireflyGraph graph) {
        return graph.getDataModel().equals(DATA_MODEL);
    }

    @Override
    public String getDataModel() {
        return DATA_MODEL;
    }

    /**
     * Function to write edge to Aerospike.
     *
     * @param edgeId     Edge id.
     * @param label      Edge label.
     * @param properties Edge properties.
     * @param inVertex   In vertex of edge.
     * @param outVertex  Out vertex of edge.
     * @return Edge.
     */
    @Override
    public FireflyEdge writeEdge(final FireflyId edgeId,
                                 final String label,
                                 final List<Map.Entry<String, Object>> properties,
                                 final FireflyVertex inVertex,
                                 final FireflyVertex outVertex) {
        /// TODO: Add generation check based writes and bulk writes.

        // Concrete example of how to add an edge between two vertices:
        //
        // First let's consider the following sets:
        //   Vertex, inVP, outVP, inIn, inOut, outIn, outOut.
        //      Vertex holds the data 1 one vertex.
        //      inVP holds the properties for the attached in vertices.
        //      outVP holds the properties for the attached out vertices.
        //      inIn holds the in edges for the in vertices.
        //      inOut holds the out edges for the in vertices.
        //      outIn holds the in edges for the out vertices.
        //      outOut holds the out edges for the out vertices.
        //
        // To add an edge to inV and outV, we must:
        //  1. Add edge to the inV bin of the outVertex record.
        //  2. Add edge to the outV bin of the inVertex record.
        //  3. Write the actual edge.
        //  4. Add properties of outVertex to in.VP of inVertex.
        //  5. Add properties of inVertex to out.VP of outVertex.
        //  6. Go through in and out edges of outVertex and add to in.in and in.out of inVertex, respectively.
        //  7. Go through in and out edges of inVertex and add to out.in and out.out of outVertex, respectively.
        //  8. Loop through the inVertex in and out edges and add an out.in and in.in edge to the outVertex.
        //  9. Loop through the outVertex in and out edges and add an out.out and in.out edge to the inVertex.


        // TODO: A future optimization by storing in/out vertex ids and using indices, we could avoid looping and just
        //  search for the in/out vertex ids as map values

        // 1, 2, and 3. These are all done by the PackedGraph writeEdge, so invoke that.
        final FireflyEdge edge = super.writeEdge(edgeId, label, properties, inVertex, outVertex);

        // 4. Add properties of outVertex to in.VP of inVertex.
        if (enableInVp) {
            StarPackedVertex.writeAdjacentProperties(db, Direction.IN, inVertex, outVertex, label);
        }

        // 5. Add properties of inVertex to out.VP of outVertex.
        if (enableOutVp) {
            StarPackedVertex.writeAdjacentProperties(db, Direction.OUT, outVertex, inVertex, label);
        }

        // 6. Go through in and out edges of outVertex and add to in.in and in.out of inVertex, respectively.
        if (enableInIn) {
            StarPackedVertex.writeCompoundEdgesOnNewEdge(db, label, inVertex, outVertex, Direction.IN, Direction.IN);
        }
        if (enableInOut) {
            StarPackedVertex.writeCompoundEdgesOnNewEdge(db, label, inVertex, outVertex, Direction.IN, Direction.OUT);
        }

        // 7. Go through in and out edges of inVertex and add to out.in and out.out of outVertex, respectively.
        if (enableOutIn) {
            StarPackedVertex.writeCompoundEdgesOnNewEdge(db, label, inVertex, outVertex, Direction.OUT, Direction.IN);
        }
        if (enableOutOut) {
            StarPackedVertex.writeCompoundEdgesOnNewEdge(db, label, inVertex, outVertex, Direction.OUT, Direction.OUT);
        }

        // 8. Loop through the inVertex in and out edges and add an out.in and in.in edge to the outVertex.
        if (enableInIn) {
            System.out.println("writing in in");
            StarPackedVertex.writeBidirectionalEdgesToAdjacentVertices(db, inVertex, outVertex, edgeId, label, Direction.IN, Direction.IN);
        }
        if (enableInOut) {
            StarPackedVertex.writeBidirectionalEdgesToAdjacentVertices(db, inVertex, outVertex, edgeId, label, Direction.IN, Direction.OUT);
        }

        // 9. Loop through the outVertex in and out edges and add an out.out and in.out edge to the inVertex.
        if (enableOutIn) {
            StarPackedVertex.writeBidirectionalEdgesToAdjacentVertices(db, inVertex, outVertex, edgeId, label, Direction.OUT, Direction.IN);
        }
        if (enableOutOut) {
            StarPackedVertex.writeBidirectionalEdgesToAdjacentVertices(db, inVertex, outVertex, edgeId, label, Direction.OUT, Direction.OUT);
        }

        return edge;
    }

    /**
     * Function to write vertex to Aerospike.
     *
     * @param idValue    Id of vertex.
     * @param label      Label of vertex.
     * @param properties List of vertex properties.
     * @return Vertex.
     */
    @Override
    public FireflyVertex writeVertex(final FireflyId idValue,
                                     final String label,
                                     final List<Map.Entry<String, Object>> properties) {
        // We only need to add this vertex itself. At this point it has no edges.
        // The actual vertex date is stored the same as the packed model so we can use that code here.
        return PackedVertex.writeVertex(this, idValue, label, properties, getTypeHint());
    }

    /**
     * Write vertex property to Aerospike.
     *
     * @param idValue FireflyId of vertex property to write.
     * @param vertex  Vertex to write property to.
     * @param key     Key of property to write.
     * @param value   Value of property to write.
     * @param <V>     Type of value to write.
     * @return FireflyVertexProperty
     */
    @Override
    public <V> FireflyVertexProperty<V> writeVertexProperty(final FireflyId idValue,
                                                            final FireflyVertex vertex,
                                                            final String key,
                                                            final V value) {
        // Write vertex property to Aerospike.
        final FireflyVertexProperty<V> fireflyVertexProperty = PackedVertexProperty.writeVertexProperty(this, vertex, idValue, key, value);

        // Append vertex property to vertex.
        vertex.writeVertexProperty(fireflyVertexProperty);

        // Append vertex property to adjacent vertex in/out property maps.
        StarPackedVertex.writeVertexPropertyToAdjacentVertices(db, vertex, fireflyVertexProperty, enableInVp, enableOutVp);

        // Return FireflyVertexProperty.
        return fireflyVertexProperty;
    }
}
