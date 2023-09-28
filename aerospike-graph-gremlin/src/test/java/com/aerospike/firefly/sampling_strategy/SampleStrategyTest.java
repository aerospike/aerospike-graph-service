package com.aerospike.firefly.sampling_strategy;

import com.aerospike.firefly.io.impl.relational.RelationalVertex;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID;
import static com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromVertex.OutputType.VERTEX_ID;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT;

public class SampleStrategyTest {

    public void load(final GraphTraversalSource g) {
        // Clear graph.
        g.V().drop().iterate();

        // Load data.
        final Vertex v1 = g.addV("entity").property("indexed", "value1").property("notindexed", "value2").next();
        for (int i = 0; i < 25000; i++) {
            try {
                final Vertex v2 = g.addV("entity-hop").
                        property("property1", "value1").
                        property("property2", "value2").
                        next();
                g.addE("has_entity").from(v1).to(v2).iterate();
            } catch (Exception e) {
                try {
                    System.out.println("??????");
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    @Test
    public void test() {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.index.vertex.properties", "indexed");
        config.setProperty(ON_RECORD_ID_LIMIT.toLowerCase(), 22000);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            //load(g);

            final List<Vertex> vertices1 = g.V().has("indexed", "value1").out().sample(30).toList();
            final List<Vertex> vertices2 = g.V().has("property1", "value1").in().sample(30).toList();
            final List<Edge> edges1 = g.V().has("indexed", "value1").outE().sample(30).toList();
            final List<Edge> edges2 = g.V().has("property1", "value1").inE().sample(30).toList();
            Assert.assertEquals(30, vertices1.size());
            Assert.assertEquals(30, vertices2.size());
            Assert.assertEquals(30, edges1.size());
            Assert.assertEquals(30, edges2.size());

            final Set<Object> vertexIds1 = vertices1.stream().map(Element::id).collect(Collectors.toSet());
            final Set<Object> vertexIds2 = vertices2.stream().map(Element::id).collect(Collectors.toSet());

            // Graph structure is 1->many and many<-1 so this should hold true.
            Assert.assertEquals(30, vertexIds1.size());
            Assert.assertEquals(1, vertexIds2.size());

            final Set<Object> edgeIds1 = edges1.stream().map(Element::id).collect(Collectors.toSet());
            final Set<Object> edgeIds2 = edges2.stream().map(Element::id).collect(Collectors.toSet());

            // Edges are unique.
            Assert.assertEquals(30, edgeIds1.size());
            Assert.assertEquals(30, edgeIds2.size());

            final List<Vertex> vertices3 = g.V().limit(30).has("property1", "value1").in().sample(30).toList();
            final List<Vertex> vertices4 = g.V().limit(29).has("property1", "value1").in().sample(30).toList();
            final List<Vertex> vertices5 = g.V().limit(31).has("property1", "value1").in().sample(30).toList();
            Assert.assertEquals(30, vertices3.size());
            Assert.assertEquals(29, vertices4.size());
            Assert.assertEquals(30, vertices5.size());

            final List<Edge> edges3 = g.V().limit(30).has("property1", "value1").inE().sample(30).toList();
            final List<Edge> edges4 = g.V().limit(29).has("property1", "value1").inE().sample(30).toList();
            final List<Edge> edges5 = g.V().limit(31).has("property1", "value1").inE().sample(30).toList();
            Assert.assertEquals(30, edges3.size());
            Assert.assertEquals(29, edges4.size());
            Assert.assertEquals(30, edges5.size());
        }
    }

    // TODO: More tests.
    @Test
    public void testSupernode() {

        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.index.vertex.properties", "indexed");
        config.setProperty(ON_RECORD_ID_LIMIT.toLowerCase(), 22000);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            //load(g);

            final RelationalVertex supernode = (RelationalVertex) g.V().has("indexed", "value1").next();

            final List<FireflyId> supernodeEdgeIds = supernode.getSupernodeIds(Direction.OUT, Set.of(), EDGE_ID);
            Assert.assertEquals(3000, supernodeEdgeIds.size());
            Assert.assertFalse(supernodeEdgeIds.stream().anyMatch(Objects::isNull));

            final List<FireflyEdge> supernodeEdges = graph.readEdges(List.of(), supernodeEdgeIds);
            Assert.assertEquals(3000, supernodeEdges.size());
            Assert.assertFalse(supernodeEdges.stream().anyMatch(Objects::isNull));

            final List<FireflyId> supernodeVertexIds = supernode.getSupernodeIds(Direction.OUT, Set.of(), VERTEX_ID);
            Assert.assertEquals(3000, supernodeVertexIds.size());
            Assert.assertFalse(supernodeVertexIds.stream().anyMatch(Objects::isNull));

            supernodeVertexIds.sort(Comparator.comparing(FireflyId::toString));
            final List<FireflyVertex> supernodeVertices = graph.readVertices(List.of(), supernodeVertexIds);
            Assert.assertEquals(3000, supernodeVertices.size());
            Assert.assertFalse(supernodeVertices.stream().anyMatch(Objects::isNull));

            final List<FireflyId> allEdgeIds = supernode.getEdgeIdsFromVertex(Direction.OUT, Set.of());
            Assert.assertEquals(25000, allEdgeIds.size());
            Assert.assertFalse(allEdgeIds.stream().anyMatch(Objects::isNull));

            final List<FireflyEdge> allEdges = graph.readEdges(List.of(), allEdgeIds);
            Assert.assertEquals(25000, allEdges.size());
            Assert.assertFalse(allEdges.stream().anyMatch(Objects::isNull));

            final List<FireflyId> allVertexIds = supernode.getVertexIdsFromVertex(Direction.OUT, Set.of());
            Assert.assertEquals(25000, allVertexIds.size());
            Assert.assertFalse(allVertexIds.stream().anyMatch(Objects::isNull));

            final List<FireflyVertex> allVertices = graph.readVertices(List.of(), allVertexIds);
            Assert.assertEquals(25000, allVertices.size());
            Assert.assertFalse(allVertices.stream().anyMatch(Objects::isNull));
        }
    }
}
