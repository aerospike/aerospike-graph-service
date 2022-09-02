package com.aerospike.firefly.io.impl.relational.star.packed;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.relational.packed.PackedGraph;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertex;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertexProperty;
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

public class StarPackedGraph extends PackedGraph {
    private static final Logger LOG = LoggerFactory.getLogger(StarPackedGraph.class);
    public static final String DATA_MODEL = "star_packed";

    /**
     * Constructor for StarLinkedGraph.
     *
     * @param db   AerospikeConnection.
     * @param conf Configuration.
     */
    public StarPackedGraph(final AerospikeConnection db, final Configuration conf) {
        super(db, conf);
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
        //  3. Write the actual edge
        // 4. Add properties of outVertex to in.VP of inVertex.
        // 5. Add properties of inVertex to out.VP of outVertex.
        // 6. Go through in and out edges of outVertex and add to in.in and in.out of inVertex, respectively.
        // 7. Go through in and out edges of inVertex and add to out.in and out.out of outVertex, respectively.
        // 8. Loop through the inVertex in and out edges and add an out.in and in.in edge to the outVertex.
        // 9. Loop through the outVertex in and out edges and add an out.out and in.out edge to the inVertex.


        // TODO: A future optimization by storing in/out vertex ids and using indices, we could avoid looping and just
        //  search for the in/out vertex ids as map values

        // 1, 2, and 3. These are all done by the PackedGraph writeEdge, so invoke that.
        final FireflyEdge edge = super.writeEdge(edgeId, label, properties, inVertex, outVertex);

        // 4. Add properties of outVertex to in.VP of inVertex.
        StarPackedVertex.writeAdjacentProperties(db, Direction.IN, inVertex, outVertex, label);

        // 5. Add properties of inVertex to out.VP of outVertex.
        StarPackedVertex.writeAdjacentProperties(db, Direction.OUT, outVertex, inVertex, label);

        // 6. Go through in and out edges of outVertex and add to in.in and in.out of inVertex, respectively.
        StarPackedVertex.writeCompoundEdgesOnNewEdge(db, inVertex, Direction.IN, label, outVertex);

        // 7. Go through in and out edges of inVertex and add to out.in and out.out of outVertex, respectively.
        StarPackedVertex.writeCompoundEdgesOnNewEdge(db, outVertex, Direction.OUT, label, inVertex);

        // 8. Loop through the inVertex in and out edges and add an out.in and in.in edge to the outVertex.
        StarPackedVertex.writeBirectionalEdgesToAdjacentVertices(db, Direction.IN, inVertex,  edgeId, label);

        // 9. Loop through the outVertex in and out edges and add an out.out and in.out edge to the inVertex.
        StarPackedVertex.writeBirectionalEdgesToAdjacentVertices(db, Direction.OUT, outVertex,  edgeId, label);

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
        StarPackedVertex.writeVertexPropertyToAdjacentVertices(db, vertex, fireflyVertexProperty);

        // Return FireflyVertexProperty.
        return fireflyVertexProperty;
    }
}
