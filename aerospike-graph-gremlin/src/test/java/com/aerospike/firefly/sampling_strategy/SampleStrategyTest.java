package com.aerospike.firefly.sampling_strategy;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.relational.RelationalVertex;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID;
import static com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromVertex.OutputType.VERTEX_ID;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ENABLE_BATCH_EDGE_READ_SAMPLING_STRATEGY;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ENABLE_COMPOSITE_ID_SAMPLING_STRATEGY;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.GLOBAL_EDGE_CACHE_ENABLED;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.junit.Assert.assertTrue;

@Ignore
public class SampleStrategyTest {

    private static final int SUPERNODE_LOAD_SIZE = 18500;
    private static final int RECORD_LIMIT = 10000;

    public void loadSimpleSupernode(final GraphTraversalSource g) {
        // Clear graph.
        g.V().drop().iterate();

        // Load data.
        final Vertex v1 = g.addV("entity").property("indexed", "value1").property("notindexed", "value2").next();
        for (int i = 0; i < SUPERNODE_LOAD_SIZE; i++) {
            final
            Vertex v2 = g.addV("entity-hop").
                    property("property1", "value1").
                    property("property2", "value2").
                    next();
            wait1Second();
            g.addE("has_entity").from(v1).to(v2).iterate();
            wait1Second();
        }
    }

    public void loadNeustarSupernode(final GraphTraversalSource g) {
        // Clear graph.
        g.V().drop().iterate();

        // Load data.
        final Vertex v1 = g.addV("entity").property("indexed", "value1").property("notindexed", "value2").next();
        for (int i = 0; i < SUPERNODE_LOAD_SIZE; i++) {
            final Vertex v2 = g.addV("entity-hop").
                    property("property1", "value1").
                    property("property2", "value2").
                    next();
            wait1Second();
            g.addE("has_entity").from(v1).to(v2).iterate();
            wait1Second();
            for (int j = 0; j < 10; j++) {
                final Vertex v3 = g.addV("entity-hop2").
                        property("property1-2", "value1").
                        property("property2-2", "value2").
                        next();
                wait1Second();
                g.addE("has_entity2").from(v2).to(v3).iterate();
            }
        }
    }

    private void wait1Second() {
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void before() {
        // Force drop each time so that config changes don"t cause issues.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        AerospikeConnection.connect(config).dropDatabase(null, true);
    }

    @Test
    public void test() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.index.vertex.properties", "indexed");
        config.setProperty(ON_RECORD_ID_LIMIT.toLowerCase(), 10);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            //loadSimpleSupernode(g);

            var x = g.V().has("indexed", "value1").out().sample(30).asAdmin();
            x.applyStrategies();
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

    @Test
    public void testSupernode() {

        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.index.vertex.properties", "indexed");
        config.setProperty(ON_RECORD_ID_LIMIT.toLowerCase(), 22000);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            loadSimpleSupernode(g);

            final RelationalVertex supernode = (RelationalVertex) g.V().has("indexed", "value1").next();

            final List<FireflyId> supernodeEdgeIds = supernode.getSupernodeIds(Direction.OUT, Set.of(), EDGE_ID);
            Assert.assertEquals(SUPERNODE_LOAD_SIZE - RECORD_LIMIT, supernodeEdgeIds.size());
            Assert.assertFalse(supernodeEdgeIds.stream().anyMatch(Objects::isNull));

            final List<FireflyEdge> supernodeEdges = graph.readEdges(List.of(), supernodeEdgeIds);
            Assert.assertEquals(SUPERNODE_LOAD_SIZE - RECORD_LIMIT, supernodeEdges.size());
            Assert.assertFalse(supernodeEdges.stream().anyMatch(Objects::isNull));

            final List<FireflyId> supernodeVertexIds = supernode.getSupernodeIds(Direction.OUT, Set.of(), VERTEX_ID);
            Assert.assertEquals(SUPERNODE_LOAD_SIZE - RECORD_LIMIT, supernodeVertexIds.size());
            Assert.assertFalse(supernodeVertexIds.stream().anyMatch(Objects::isNull));

            supernodeVertexIds.sort(Comparator.comparing(FireflyId::toString));
            final List<FireflyVertex> supernodeVertices = graph.readVertices(List.of(), supernodeVertexIds);
            Assert.assertEquals(SUPERNODE_LOAD_SIZE - RECORD_LIMIT, supernodeVertices.size());
            Assert.assertFalse(supernodeVertices.stream().anyMatch(Objects::isNull));

            final List<FireflyId> allEdgeIds = supernode.getEdgeIdsFromVertex(Direction.OUT, Set.of());
            Assert.assertEquals(SUPERNODE_LOAD_SIZE, allEdgeIds.size());
            Assert.assertFalse(allEdgeIds.stream().anyMatch(Objects::isNull));

            final List<FireflyEdge> allEdges = graph.readEdges(List.of(), allEdgeIds);
            Assert.assertEquals(SUPERNODE_LOAD_SIZE, allEdges.size());
            Assert.assertFalse(allEdges.stream().anyMatch(Objects::isNull));

            final List<FireflyId> allVertexIds = supernode.getVertexIdsFromVertex(Direction.OUT, Set.of());
            Assert.assertEquals(SUPERNODE_LOAD_SIZE, allVertexIds.size());
            Assert.assertFalse(allVertexIds.stream().anyMatch(Objects::isNull));
            Assert.assertEquals(SUPERNODE_LOAD_SIZE, new HashSet<>(allVertexIds).size());

            final List<FireflyVertex> allVertices = graph.readVertices(List.of(), allVertexIds);
            Assert.assertEquals(SUPERNODE_LOAD_SIZE, allVertices.size());
            Assert.assertFalse(allVertices.stream().anyMatch(Objects::isNull));
            Assert.assertEquals(SUPERNODE_LOAD_SIZE, new HashSet<>(allVertices).size());
        }
    }

    @Test
    public void testNeustarPerformance() {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.index.vertex.properties", "indexed");
        config.setProperty(ON_RECORD_ID_LIMIT.toLowerCase(), 10);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            //loadNeustarSupernode(g);

            for (int i = 0; i < 20; i++) {
                System.out.println(
                        g.V().
                        has("indexed", "value1").
                        has("notindexed", "value2").
                        out("has_entity").
                        sample(30).
                        outE("has_entity2").
                        sample(30).
                        subgraph("a").
                        cap("a").profile().next());
            }
        }

    }

    // Add testing around has(..).sample(..) and sample(..).has(..)
    @Test
    public void testFSF(){


        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.index.vertex.properties", "indexed");
        config.setProperty(ON_RECORD_ID_LIMIT.toLowerCase(), 0);

        //
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();
            GraphHelper.cloneElements(TinkerFactory.createGratefulDead(), graph);
            //Traversal traversal = g.V().repeat(both().simplePath()).times(3).path();
            //traversal.asAdmin().applyStrategies();
            //System.out.println(traversal);
            //long counter = 0;
            //while (traversal.hasNext()) {
            //    counter++;
            //    assertTrue(((Path)traversal.next()).isSimple());
            //}

            List<Vertex> vertices = g.V().toList();
            final Vertex v1 = g.V(325).next();
            List<?> verticesOut = g.V(v1.id()).outE().toList();
            //List<?> verticesIn = g.V(v1.id()).inE().toList();
            List<?> verticesOut2 = g.V(v1.id()).out().toList();
            //List<?> verticesIn2 = g.V(v1.id()).in().toList();
                //System.out.println(verticesOut.size());
                //System.out.println(verticesIn.size());
                //System.out.println(verticesOut2.size());
                //System.out.println(verticesIn2.size());
            if (verticesOut.size() != verticesOut2.size()) {
                System.out.println("mismatch out!=ouE" + verticesOut.size() + " != " + verticesOut2.size());
            }
            //if (verticesIn.size() != verticesIn2.size()) {
            //    System.out.println("mismatch in!=inE" + verticesIn.size() + " != " + verticesIn2.size());
            //}
            System.out.println("8049");
        }
    }
    // Output size: 12
    //Output: [v[1], v[1], v[1], v[2], v[3], v[3], v[3], v[4], v[4], v[4], v[5], v[6]]
    //Output size: 30
    //Output: [v[1], v[1], v[1], v[1], v[1], v[1], v[1], v[2], v[2], v[2], v[3], v[3], v[3], v[3], v[3], v[3], v[3], v[4], v[4], v[4], v[4], v[4], v[4], v[4], v[5], v[5], v[5], v[6], v[6], v[6]]
    //Output size: 42
    //Output: [v[1], v[1], v[1], v[1], v[1], v[1], v[1], v[1], v[1], v[1], v[2], v[2], v[2], v[2], v[3], v[3], v[3], v[3], v[3], v[3], v[3], v[3], v[3], v[3], v[4], v[4], v[4], v[4], v[4], v[4], v[4], v[4], v[4], v[4], v[5], v[5], v[5], v[5], v[6], v[6], v[6], v[6]]
    //32
    //[path[v[1], v[3], v[4], v[5]], path[v[1], v[3], v[4], v[5]], path[v[1], v[4], v[3], v[6]], path[v[1], v[4], v[3], v[6]], path[v[4], v[1], v[3], v[6]], path[v[4], v[1], v[3], v[6]], path[v[4], v[3], v[1], v[2]], path[v[4], v[3], v[1], v[2]], path[v[6], v[3], v[1], v[2]], path[v[6], v[3], v[1], v[4]], path[v[6], v[3], v[4], v[5]], path[v[6], v[3], v[4], v[5]], path[v[6], v[3], v[4], v[1]], path[v[6], v[3], v[4], v[1]], path[v[5], v[4], v[1], v[2]], path[v[5], v[4], v[1], v[2]], path[v[5], v[4], v[1], v[3]], path[v[5], v[4], v[1], v[3]], path[v[5], v[4], v[3], v[6]], path[v[5], v[4], v[3], v[6]], path[v[5], v[4], v[3], v[1]], path[v[5], v[4], v[3], v[1]], path[v[3], v[1], v[4], v[5]], path[v[3], v[1], v[4], v[5]], path[v[3], v[4], v[1], v[2]], path[v[3], v[4], v[1], v[2]], path[v[2], v[1], v[3], v[6]], path[v[2], v[1], v[3], v[4]], path[v[2], v[1], v[4], v[5]], path[v[2], v[1], v[4], v[5]], path[v[2], v[1], v[4], v[3]], path[v[2], v[1], v[4], v[3]]]
    //[path[v[1], v[3], v[4], v[5]], path[v[1], v[4], v[3], v[6]], path[v[6], v[3], v[1], v[2]], path[v[6], v[3], v[1], v[4]], path[v[6], v[3], v[4], v[5]], path[v[6], v[3], v[4], v[1]], path[v[5], v[4], v[3], v[1]], path[v[5], v[4], v[3], v[6]], path[v[5], v[4], v[1], v[3]], path[v[5], v[4], v[1], v[2]], path[v[3], v[1], v[4], v[5]], path[v[3], v[4], v[1], v[2]], path[v[4], v[3], v[1], v[2]], path[v[4], v[1], v[3], v[6]], path[v[2], v[1], v[3], v[4]], path[v[2], v[1], v[3], v[6]], path[v[2], v[1], v[4], v[5]], path[v[2], v[1], v[4], v[3]]]
}
