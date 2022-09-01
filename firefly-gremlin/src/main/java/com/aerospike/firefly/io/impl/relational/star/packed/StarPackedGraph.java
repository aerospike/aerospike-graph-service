package com.aerospike.firefly.io.impl.relational.star.packed;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.relational.packed.PackedGraph;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertex;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertexProperty;
import com.aerospike.firefly.io.impl.relational.star.StarGraph;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class StarPackedGraph extends PackedGraph implements StarGraph {
    private static final Logger LOG = LoggerFactory.getLogger(StarPackedGraph.class);
    public static final String DATA_MODEL = "StarLinked";

    /**
     * Constructor for StarLinkedGraph.
     *
     * @param db   AerospikeConnection.
     * @param conf Configuration.
     */
    public StarPackedGraph(final AerospikeConnection db, final Configuration conf) {
        super(db, conf);
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
        //   Let each set have key 1 for inVertex and key 2 for outVertex (as passed into function)
        //
        // To add an edge to inV and outV, we must:
        //  1. Add edge to the inV bin of the outVertex record (record key 2 of vertex set)
        //  2. Add edge to the outV bin of the inVertex record (record key 1 of vertex set)
        //  3. Add inVertex properties to outVertex property set (record key 2 of inVP set)
        //  4. Add outVertex properties to inVertex property set (record key 1 of outVP set)
        //  5. For inVertex inEdges, add outVertex inEdges to inIn
        //  6. For inVertex inEdges, add outVertex outEdges to inOut
        //  7. For outVertex inEdges, add inVertex inEdges to outIn
        //  8. For outVertex inEdges, add outVertex outEdges to outOut
        //  9. Loop through inVertex inVertices and add edge to inIn records.
        // 10. Loop through outVertex outVertices and add edge to outOut records.
        // 11. Loop through inVertex outVertices and add edge to inOut records.
        // 12. Loop through outVertex inVertices and add edge to outIn records.
        // 13. Write the actual edge.

        // TODO: A future optimization by storing in/out vertex ids and using indices, we could avoid looping and just
        //  search for the in/out vertex ids as map values

        // 1. Add edge to the inV bin of the outVertex record.
        outVertex.writeEdge(Direction.OUT, edgeId, label);

        // 2. Add edge to the outV bin of the inVertex record.
        inVertex.writeEdge(Direction.IN, edgeId, label);

        // 3. Add inVertex properties to outVertex property set.
        StarPackedVertex.writeAdjacentProperties(db, Direction.IN, inVertex, outVertex, label);

        // 4. Add outVertex properties to inVertex property set.
        StarPackedVertex.writeAdjacentProperties(db, Direction.OUT, outVertex, inVertex, label);

        // 5 and 6 (function adds adjacent vertices in and out to input direction of input vertex).
        StarPackedVertex.writeCompoundEdgesOnNewEdge(db, inVertex, Direction.IN, label, outVertex);

        // 7 and 8 (function adds adjacent vertices in and out to input direction of input vertex).
        StarPackedVertex.writeCompoundEdgesOnNewEdge(db, outVertex, Direction.OUT, label, inVertex);

        // 9. Loop through inVertex inEdges and add edge to inIn records.
        StarPackedVertex.writeAdjacentEdgesToAdjacentVertex(db, Direction.IN, inVertex, Direction.IN, edgeId, label);

        // 10. Loop through outVertex outVertices and add edge to outOut records.
        StarPackedVertex.writeAdjacentEdgesToAdjacentVertex(db, Direction.OUT, outVertex, Direction.OUT, edgeId, label);

        // 11. Loop through inVertex outVertices and add edge to inOut records.
        StarPackedVertex.writeAdjacentEdgesToAdjacentVertex(db, Direction.IN, inVertex, Direction.OUT, edgeId, label);

        // 12. Loop through outVertex inVertices and add edge to outIn records.
        StarPackedVertex.writeAdjacentEdgesToAdjacentVertex(db, Direction.OUT, outVertex, Direction.IN, edgeId, label);

        // 13. Write the edge to the edge set.
        return super.writeEdge(edgeId, label, properties, inVertex, outVertex);
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
        StarPackedVertex.writeVertexPropertyToAdjacentVertices(db, vertex, fireflyVertexProperty);

        // Return FireflyVertexProperty.
        return fireflyVertexProperty;
    }
}
