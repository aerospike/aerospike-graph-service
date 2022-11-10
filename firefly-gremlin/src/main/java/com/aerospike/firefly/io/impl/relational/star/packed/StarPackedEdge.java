package com.aerospike.firefly.io.impl.relational.star.packed;

import com.aerospike.firefly.io.impl.relational.RelationalEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Direction;


/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class StarPackedEdge extends RelationalEdge {
     public StarPackedEdge(final FireflyId fid,
                           final String label,
                           final FireflyGraph graph,
                           final FireflyId outVertex,
                           final FireflyId inVertex) {
        super(fid, label, graph, outVertex, inVertex);
    }

    @Override
    public void removeEdge() {
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
        final StarPackedGraph graph = (StarPackedGraph) this.graph;
        if (graph.enableOutVp) {
            StarPackedVertex.removeAdjacentVertexPropertiesFromVertex(db, this, Direction.OUT);
        }

        // 2. Find the properties of the in vertex in the inVP set of the out vertex and remove it.
        if (graph.enableInVp) {
            StarPackedVertex.removeAdjacentVertexPropertiesFromVertex(db, this, Direction.IN);
        }

        // 3/4. Loop through the inVertex/outVertex in and out edges and remove appropriate in.in/out.in and in.out/out.out paths to adjacent vertices.
        StarPackedVertex.removeCompoundEdgesFromAdjacentVertices(db, this, graph.enableOutOut, graph.enableOutIn, graph.enableInOut, graph.enableInIn);

        // 5/6. Remove in.in and in.out paths that go through outVertex from inVertex and out.in and out.out paths that go through inVertex from outVertex.
        StarPackedVertex.removeCompoundEdges(db, this, graph.enableOutOut, graph.enableOutIn, graph.enableInOut, graph.enableInIn);

        // Remove the edge itself.
        super.removeEdge();
    }
}
