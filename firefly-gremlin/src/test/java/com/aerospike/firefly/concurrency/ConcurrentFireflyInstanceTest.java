package com.aerospike.firefly.concurrency;

import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedGraph;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class ConcurrentFireflyInstanceTest {
    final static int INITIAL_COUNT = 1250;
    final static int THREAD_COUNT = 4;
    final static int ADD_REMOVE_COUNT = 250;

    @Before
    public void setup() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            Assume.assumeFalse(StarPackedGraph.isStarPackedGraph(fireflyGraph));
        }
    }

    @Test
    public void concurrentFireflyInstanceEdgeAdditionTest() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            fireflyGraph.getBaseGraph().dropDatabase();
            final GraphTraversalSource g = fireflyGraph.traversal();
            final FireflyVertex a = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createId(1, FireflyVertex.class), "foo", new ArrayList<>());
            final FireflyVertex b = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createId(2, FireflyVertex.class), "foo", new ArrayList<>());
            for (int i = 0; i < INITIAL_COUNT; i++) {
                fireflyGraph.writeEdge(fireflyGraph.getIdFactory().createId(i, FireflyEdge.class), "bar", new ArrayList<>(), a, b);
            }
            final CyclicBarrier gate = new CyclicBarrier(THREAD_COUNT + 1);
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
            for (int i = 0; i < THREAD_COUNT; i++) {
                final int finalI = i;
                executorService.submit(() -> concurrentFireflyEdgeWrite(fireflyGraph, a, b, finalI, gate));
            }
            gate.await();
            for (int i = 0; i < ADD_REMOVE_COUNT; i++) {
                final FireflyEdge edge = fireflyGraph.writeEdge(fireflyGraph.getIdFactory().createId(INITIAL_COUNT + i + 100 * ADD_REMOVE_COUNT, FireflyEdge.class), "bar", new ArrayList<>(), a, b);
            }
            executorService.shutdown();
            executorService.awaitTermination(30, TimeUnit.SECONDS);
            final Long outEdgeCount = g.V(a.id()).inE().count().next();
            final Long inEdgeCount = g.V(b.id()).outE().count().next();
            Assert.assertEquals(INITIAL_COUNT + (THREAD_COUNT + 1) * ADD_REMOVE_COUNT, outEdgeCount.longValue());
            Assert.assertEquals(INITIAL_COUNT + (THREAD_COUNT + 1) * ADD_REMOVE_COUNT, inEdgeCount.longValue());
            Set<Object> inEdges = g.V(a.id()).inE().id().toSet();
            Set<Object> outEdges = g.V(b.id()).outE().id().toSet();
            Assert.assertEquals(INITIAL_COUNT + (THREAD_COUNT + 1) * ADD_REMOVE_COUNT, outEdges.size());
            Assert.assertEquals(outEdges, inEdges);
        } catch (BrokenBarrierException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void concurrentFireflyInstanceEdgeRemovalTest() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            fireflyGraph.getBaseGraph().dropDatabase();
            final GraphTraversalSource g = fireflyGraph.traversal();
            final FireflyVertex a = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createId(1, FireflyVertex.class), "foo", new ArrayList<>());
            final FireflyVertex b = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createId(2, FireflyVertex.class), "foo", new ArrayList<>());
            for (int i = 0; i < INITIAL_COUNT; i++) {
                fireflyGraph.writeEdge(fireflyGraph.getIdFactory().createId(i, FireflyEdge.class), "bar", new ArrayList<>(), a, b);
            }
            final CyclicBarrier gate = new CyclicBarrier(THREAD_COUNT + 1);
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
            for (int i = 0; i < THREAD_COUNT; i++) {
                final int finalI = i;
                executorService.submit(() -> concurrentFireflyEdgeRemoval(fireflyGraph, finalI, gate));
            }
            gate.await();
            for (int i = 0; i < ADD_REMOVE_COUNT; i++) {
                g.E(fireflyGraph.getIdFactory().createId(i + THREAD_COUNT * ADD_REMOVE_COUNT, FireflyEdge.class)).drop().iterate();
            }
            executorService.shutdown();
            executorService.awaitTermination(30, TimeUnit.SECONDS);
            final Long outEdgeCount = g.V(a.id()).inE().count().next();
            final Long inEdgeCount = g.V(b.id()).outE().count().next();
            Assert.assertEquals(INITIAL_COUNT - (THREAD_COUNT + 1) * ADD_REMOVE_COUNT, outEdgeCount.longValue());
            Assert.assertEquals(INITIAL_COUNT - (THREAD_COUNT + 1) * ADD_REMOVE_COUNT, inEdgeCount.longValue());
            Set<Object> inEdges = g.V(a.id()).inE().id().toSet();
            Set<Object> outEdges = g.V(b.id()).outE().id().toSet();
            Assert.assertEquals(INITIAL_COUNT - (THREAD_COUNT + 1) * ADD_REMOVE_COUNT, outEdges.size());
            Assert.assertEquals(outEdges, inEdges);
        } catch (BrokenBarrierException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void concurrentFireflyInstanceEdgeAddRemoveTest() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            fireflyGraph.getBaseGraph().dropDatabase();
            final GraphTraversalSource g = fireflyGraph.traversal();
            final FireflyVertex a = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createId(1, FireflyVertex.class), "foo", new ArrayList<>());
            final FireflyVertex b = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createId(2, FireflyVertex.class), "foo", new ArrayList<>());
            for (int i = 0; i < INITIAL_COUNT; i++) {
                fireflyGraph.writeEdge(fireflyGraph.getIdFactory().createId(i, FireflyEdge.class), "bar", new ArrayList<>(), a, b);
            }
            final CyclicBarrier gate = new CyclicBarrier((2 * THREAD_COUNT) + 1);
            final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT * 2);
            for (int i = 0; i < THREAD_COUNT; i++) {
                final int finalI = i;
                executorService.submit(() -> concurrentFireflyEdgeWrite(fireflyGraph, a, b, finalI, gate));
                executorService.submit(() -> concurrentFireflyEdgeRemoval(fireflyGraph, finalI, gate));
            }
            gate.await();
            for (int i = 0; i < ADD_REMOVE_COUNT; i++) {
                fireflyGraph.writeEdge(fireflyGraph.getIdFactory().createId(INITIAL_COUNT + i + 100 * ADD_REMOVE_COUNT, FireflyEdge.class), "bar", new ArrayList<>(), a, b);
            }
            executorService.shutdown();
            executorService.awaitTermination(30, TimeUnit.SECONDS);
            final Long outEdgeCount = g.V(a.id()).inE().count().next();
            final Long inEdgeCount = g.V(b.id()).outE().count().next();
            final Long actualEdgeCount = g.E().count().next();
            Assert.assertEquals(INITIAL_COUNT + ADD_REMOVE_COUNT, outEdgeCount.longValue());
            Assert.assertEquals(INITIAL_COUNT + ADD_REMOVE_COUNT, inEdgeCount.longValue());
            Assert.assertEquals(INITIAL_COUNT + ADD_REMOVE_COUNT, actualEdgeCount.longValue());
            final Set<Object> inEdges = g.V(a.id()).inE().id().toSet();
            final Set<Object> outEdges = g.V(b.id()).outE().id().toSet();
            final Set<Object> edgeIds = g.E().id().toSet();
            Assert.assertEquals(INITIAL_COUNT + ADD_REMOVE_COUNT, outEdges.size());
            Assert.assertEquals(outEdges, inEdges);
            Assert.assertEquals(outEdges, edgeIds);
        } catch (BrokenBarrierException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void concurrentFireflyInstancePropertyAddTest() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            fireflyGraph.getBaseGraph().dropDatabase();
            final GraphTraversalSource g = fireflyGraph.traversal();
            final FireflyVertex a = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createId(1, FireflyVertex.class), "foo", new ArrayList<>());
            final FireflyVertex b = (FireflyVertex) g.V().next();
            final FireflyVertex c = (FireflyVertex) g.V().next();
            final FireflyVertex d = (FireflyVertex) g.V().next();
            final FireflyVertex e = (FireflyVertex) g.V().next();
            for (int i = 0; i < INITIAL_COUNT; i++) {
                fireflyGraph.writeVertexProperty(fireflyGraph.getIdFactory().createId(i, FireflyVertexProperty.class), a, String.format("%d", i), i);
            }
            final CyclicBarrier gate = new CyclicBarrier(THREAD_COUNT + 1);
            final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
            for (int i = 0; i < THREAD_COUNT; i++) {
                final int finalI = i;
                if (i == 0) {
                    executorService.submit(() -> concurrentFireflyPropertyWrite(fireflyGraph, a, finalI, gate));
                } else if (i == 1) {
                    executorService.submit(() -> concurrentFireflyPropertyWrite(fireflyGraph, b, finalI, gate));
                } else if (i == 2) {
                    executorService.submit(() -> concurrentFireflyPropertyWrite(fireflyGraph, c, finalI, gate));
                } else if (i == 3) {
                    executorService.submit(() -> concurrentFireflyPropertyWrite(fireflyGraph, d, finalI, gate));
                }
            }
            gate.await();
            for (int i = 0; i < ADD_REMOVE_COUNT; i++) {
                final int actualIdx = INITIAL_COUNT + i + THREAD_COUNT * ADD_REMOVE_COUNT;
                fireflyGraph.writeVertexProperty(fireflyGraph.getIdFactory().createId(actualIdx, FireflyVertexProperty.class), e, String.format("%d", actualIdx), actualIdx);
            }
            executorService.shutdown();
            executorService.awaitTermination(30, TimeUnit.SECONDS);
            final Long propertyCount = g.V(a.id()).properties().count().next();
            final List<String> propertyIds = g.V(a.id()).properties().key().toList();
            for (int i = 0; i < INITIAL_COUNT + (THREAD_COUNT + 1) * ADD_REMOVE_COUNT; i++) {
                Assert.assertTrue(propertyIds.contains(String.format("%d", i)));
            }
            Assert.assertEquals(INITIAL_COUNT + (THREAD_COUNT + 1) * ADD_REMOVE_COUNT, propertyCount.longValue());
            final Map<String, Object> properties = g.V(a.id()).propertyMap().next();
            Assert.assertEquals(INITIAL_COUNT + (THREAD_COUNT + 1) * ADD_REMOVE_COUNT, properties.size());
        } catch (BrokenBarrierException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void concurrentFireflyInstancePropertyRemoveTest() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            fireflyGraph.getBaseGraph().dropDatabase();
            final GraphTraversalSource g = fireflyGraph.traversal();
            final FireflyVertex a = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createId(1,FireflyVertex.class), "foo", new ArrayList<>());
            for (int i = 0; i < INITIAL_COUNT; i++) {
                fireflyGraph.writeVertexProperty(fireflyGraph.getIdFactory().createId(i, FireflyVertexProperty.class), a, String.format("%d", i), i);
            }
            final CyclicBarrier gate = new CyclicBarrier(THREAD_COUNT + 1);
            final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
            for (int i = 0; i < THREAD_COUNT; i++) {
                final int finalI = i;
                executorService.submit(() -> concurrentFireflyPropertyRemove(fireflyGraph, finalI, gate));
            }

            gate.await();
            for (int i = 0; i < ADD_REMOVE_COUNT; i++) {
                final int actualIdx = i + THREAD_COUNT * ADD_REMOVE_COUNT;
                g.V().limit(1).properties(String.format("%d", actualIdx)).drop().iterate();
            }
            executorService.shutdown();
            executorService.awaitTermination(30, TimeUnit.SECONDS);
            final Long propertyCount = g.V(a.id()).properties().count().next();
            Assert.assertEquals(0, propertyCount.longValue());
            final Map<String, Object> properties = g.V(a.id()).propertyMap().next();
            Assert.assertEquals(0, properties.size());
        } catch (BrokenBarrierException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void concurrentFireflyInstancePropertyAddRemoveTest() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            fireflyGraph.getBaseGraph().dropDatabase();
            final GraphTraversalSource g = fireflyGraph.traversal();
            final FireflyVertex a = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createId(1,FireflyVertex.class), "foo", new ArrayList<>());
            final FireflyVertex b = (FireflyVertex) g.V().next();
            final FireflyVertex c = (FireflyVertex) g.V().next();
            final FireflyVertex d = (FireflyVertex) g.V().next();
            final FireflyVertex e = (FireflyVertex) g.V().next();
            for (int i = 0; i < INITIAL_COUNT; i++) {
                fireflyGraph.writeVertexProperty(fireflyGraph.getIdFactory().createId(i, FireflyVertexProperty.class), a, String.format("%d", i), i);
            }
            final CyclicBarrier gate = new CyclicBarrier((2 * THREAD_COUNT) + 1);
            final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT * 2);
            for (int i = 0; i < THREAD_COUNT; i++) {
                final int finalI = i;
                if (i == 0) {
                    executorService.submit(() -> concurrentFireflyPropertyWrite(fireflyGraph, a, finalI, gate));
                } else if (i == 1) {
                    executorService.submit(() -> concurrentFireflyPropertyWrite(fireflyGraph, b, finalI, gate));
                } else if (i == 2) {
                    executorService.submit(() -> concurrentFireflyPropertyWrite(fireflyGraph, c, finalI, gate));
                } else if (i == 3) {
                    executorService.submit(() -> concurrentFireflyPropertyWrite(fireflyGraph, d, finalI, gate));
                }
            }
            for (int i = 0; i < THREAD_COUNT; i++) {
                final int finalI = i;
                executorService.submit(() -> concurrentFireflyPropertyRemove(fireflyGraph, finalI, gate));
            }
            gate.await();
            for (int i = 0; i < ADD_REMOVE_COUNT; i++) {
                final int actualIdx = i + THREAD_COUNT * ADD_REMOVE_COUNT;
                g.V().limit(1).properties(String.format("%d", actualIdx)).drop().iterate();
            }
            executorService.shutdown();
            if (!executorService.awaitTermination(90, TimeUnit.SECONDS)) {
                Assert.fail("Failed to terminate in 90 seconds.");
            }
            final Long propertyCount = g.V(a.id()).properties().count().next();
            final List<String> propertyIds = g.V(a.id()).properties().key().toList();
            for (int i = INITIAL_COUNT; i < INITIAL_COUNT + THREAD_COUNT * ADD_REMOVE_COUNT; i++) {
                Assert.assertTrue(propertyIds.contains(String.format("%d", i)));
            }
            Assert.assertEquals(ADD_REMOVE_COUNT * THREAD_COUNT, propertyCount.longValue());
            final Map<String, Object> properties = g.V(a.id()).propertyMap().next();
            Assert.assertEquals(ADD_REMOVE_COUNT * THREAD_COUNT, properties.size());
        } catch (BrokenBarrierException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void concurrentFireflyEdgeRemoval(final FireflyGraph fireflyGraph, int threadIdx, final CyclicBarrier gate) {
        try {
            gate.await();
            final GraphTraversalSource g = fireflyGraph.traversal();
            for (int i = 0; i < ADD_REMOVE_COUNT; i++) {
                g.E(fireflyGraph.getIdFactory().createId(i + threadIdx * ADD_REMOVE_COUNT, FireflyEdge.class)).drop().iterate();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void concurrentFireflyEdgeWrite(final FireflyGraph graph, final FireflyVertex vertexA, final FireflyVertex vertexB, final int threadId, final CyclicBarrier gate) {
        try {
            gate.await();
            for (int i = 0; i < ADD_REMOVE_COUNT; i++) {
                graph.writeEdge(graph.getIdFactory().createId(INITIAL_COUNT + i + threadId * ADD_REMOVE_COUNT,FireflyEdge.class), "bar", new ArrayList<>(), vertexA, vertexB);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void concurrentFireflyPropertyWrite(final FireflyGraph graph, final FireflyVertex vertex, final int threadId, final CyclicBarrier gate) {
        int i = 0;
        try {
            gate.await();
            for (; i < ADD_REMOVE_COUNT; i++) {
                final int actualIdx = INITIAL_COUNT + i + threadId * ADD_REMOVE_COUNT;
                final FireflyId id = graph.getIdFactory().createId(actualIdx,FireflyVertexProperty.class);
                graph.writeVertexProperty(id, vertex, String.format("%d", actualIdx), actualIdx);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void concurrentFireflyPropertyRemove(final FireflyGraph graph, final int threadId, final CyclicBarrier gate) {
        int i = 0;
        try {
            gate.await();
            for (; i < ADD_REMOVE_COUNT; i++) {
                final int actualIdx = i + threadId * ADD_REMOVE_COUNT;
                final FireflyId id = graph.getIdFactory().createId(actualIdx,FireflyVertexProperty.class);
                graph.traversal().V().limit(1).properties(String.format("%d", actualIdx)).drop().iterate();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
