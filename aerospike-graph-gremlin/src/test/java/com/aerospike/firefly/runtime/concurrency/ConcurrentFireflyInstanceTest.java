package com.aerospike.firefly.runtime.concurrency;

import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class ConcurrentFireflyInstanceTest {
    final static int INITIAL_COUNT = 1250;
    final static int THREAD_COUNT = 4;
    final static int ADD_REMOVE_COUNT = 250;

    @Before
    public void beforeEach() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, true);
        }
    }

    private static byte[] getBytesId(final long value) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        final byte[] id = new byte[16];
        System.arraycopy(buffer.array(), 0, id, 0, 8);
        return id;
    }

    @Test
    public void concurrentFireflyInstanceEdgeAdditionTest() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);
            final GraphTraversalSource g = fireflyGraph.traversal();
            final FireflyVertex a = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createVertexId(1), "foo", new ArrayList<>());
            final FireflyVertex b = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createVertexId(2), "foo", new ArrayList<>());
            for (int i = 0; i < INITIAL_COUNT; i++) {
                final byte[] id = getBytesId(i);
                fireflyGraph.getAerospikeOperations().writeEdge(fireflyGraph.getIdFactory().createEdgeId(id), "bar", new ArrayList<>(), a, b);
            }
            final CyclicBarrier gate = new CyclicBarrier(THREAD_COUNT + 1);
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
            for (int i = 0; i < THREAD_COUNT; i++) {
                final int finalI = i;
                executorService.submit(() -> concurrentFireflyEdgeWrite(fireflyGraph, a, b, finalI, gate));
            }
            gate.await();
            for (int i = 0; i < ADD_REMOVE_COUNT; i++) {
                final byte[] id = getBytesId(INITIAL_COUNT + i + 100 * ADD_REMOVE_COUNT);
                final FireflyEdge edge = fireflyGraph.getAerospikeOperations().writeEdge(fireflyGraph.getIdFactory().createEdgeId(id), "bar", new ArrayList<>(), a, b);
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
            fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);
            final GraphTraversalSource g = fireflyGraph.traversal();
            final FireflyVertex a = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createVertexId(1), "foo", new ArrayList<>());
            final FireflyVertex b = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createVertexId(2), "foo", new ArrayList<>());
            for (int i = 0; i < INITIAL_COUNT; i++) {
                final byte[] id = getBytesId(i);
                fireflyGraph.getAerospikeOperations().writeEdge(fireflyGraph.getIdFactory().createEdgeId(id), "bar", new ArrayList<>(), a, b);
            }
            final CyclicBarrier gate = new CyclicBarrier(THREAD_COUNT + 1);
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
            for (int i = 0; i < THREAD_COUNT; i++) {
                final int finalI = i;
                executorService.submit(() -> concurrentFireflyEdgeRemoval(fireflyGraph, finalI, gate));
            }
            gate.await();
            for (int i = 0; i < ADD_REMOVE_COUNT; i++) {
                final byte[] id = getBytesId(i + THREAD_COUNT * ADD_REMOVE_COUNT);
                g.E(id).drop().iterate();
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
            fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);
            final GraphTraversalSource g = fireflyGraph.traversal();
            final FireflyVertex a = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createVertexId(1), "foo", new ArrayList<>());
            final FireflyVertex b = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createVertexId(2), "foo", new ArrayList<>());
            for (int i = 0; i < INITIAL_COUNT; i++) {
                final byte[] id = getBytesId(i);
                fireflyGraph.getAerospikeOperations().writeEdge(fireflyGraph.getIdFactory().createEdgeId(id), "bar", new ArrayList<>(), a, b);
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
                final byte[] id = getBytesId(INITIAL_COUNT + i + 100 * ADD_REMOVE_COUNT);
                fireflyGraph.getAerospikeOperations().writeEdge(fireflyGraph.getIdFactory().createEdgeId(id), "bar", new ArrayList<>(), a, b);
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
            fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);
            final GraphTraversalSource g = fireflyGraph.traversal();
            final FireflyVertex a = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createVertexId(1), "foo", new ArrayList<>());
            final FireflyVertex b = (FireflyVertex) g.V().next();
            final FireflyVertex c = (FireflyVertex) g.V().next();
            final FireflyVertex d = (FireflyVertex) g.V().next();
            final FireflyVertex e = (FireflyVertex) g.V().next();
            for (int i = 0; i < INITIAL_COUNT; i++) {
                a.property(VertexProperty.Cardinality.single, String.format("%d", i), i);
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
                a.property(VertexProperty.Cardinality.single, String.format("%d", actualIdx), actualIdx);
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
            fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);
            final GraphTraversalSource g = fireflyGraph.traversal();
            final FireflyVertex a = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createVertexId(1), "foo", new ArrayList<>());
            for (int i = 0; i < INITIAL_COUNT; i++) {
                a.property(VertexProperty.Cardinality.single, String.format("%d", i), i);
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
            fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);
            final GraphTraversalSource g = fireflyGraph.traversal();
            final FireflyVertex a = fireflyGraph.writeVertex(fireflyGraph.getIdFactory().createVertexId(1), "foo", new ArrayList<>());
            final FireflyVertex b = (FireflyVertex) g.V().next();
            final FireflyVertex c = (FireflyVertex) g.V().next();
            final FireflyVertex d = (FireflyVertex) g.V().next();
            final FireflyVertex e = (FireflyVertex) g.V().next();
            for (int i = 0; i < INITIAL_COUNT; i++) {
                a.property(VertexProperty.Cardinality.single, String.format("%d", i), i);
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

    @Test
    public void concurrentFireflyInstanceMultiPropertyTest() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);
            final GraphTraversalSource g = fireflyGraph.traversal();
            final Object id = g.addV("foo").next().id();
            final AtomicBoolean isRunning = new AtomicBoolean(true);
            final AtomicBoolean firstPropertyAdded = new AtomicBoolean(false);
            final Random rng = new Random();
            final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT * 2);
            for (int i = 0; i < THREAD_COUNT * 2; i++) {
                executorService.submit(() -> {
                    while (isRunning.get()) {
                        final long randomNumber = rng.nextInt(100);
                        if (randomNumber < 5) {
                            g.V(id).property(VertexProperty.Cardinality.single, "test", -1L).iterate();
                            firstPropertyAdded.set(true);
                        } else {
                            g.V(id).property(VertexProperty.Cardinality.list, "test", randomNumber).iterate();
                            firstPropertyAdded.set(true);
                        }
                    }
                });
            }
            int successes = 0;
            boolean seenSingle = false;
            boolean seenMulti = false;
            boolean wereVpsRemovedBySingle = false;
            int lastVpCount = 0;
            while (successes < 1000) {
                while (!firstPropertyAdded.get()) {
                    // Wait until this happens
                }
                final GraphTraversal properties = g.V(id).properties("test");
                boolean isMulti = false;
                long singleVpId = Long.MAX_VALUE;
                int currentVpCount = 0;
                while (properties.hasNext()) {
                    final VertexProperty<Long> test = (VertexProperty<Long>) properties.next();
                    if (test.value() == -1L) {
                        Assert.assertEquals(Long.MAX_VALUE, singleVpId);
                        singleVpId = (long) test.id();
                    } else {
                        Assert.assertTrue(test.value() >= 5 && test.value() < 100);
                        if (isMulti) {
                            seenMulti = true;
                        }
                        isMulti = true;
                    }

                    if (!properties.hasNext()) {
                        if (singleVpId != Long.MIN_VALUE) {
                            seenSingle = true;
                        }
                    }
                    currentVpCount++;
                }
                if (currentVpCount < lastVpCount) {
                    wereVpsRemovedBySingle = true;
                }
                lastVpCount = currentVpCount;
                successes++;
            }
            isRunning.set(false);
            executorService.shutdown();
            Assert.assertTrue(seenSingle);
            Assert.assertTrue(seenMulti);
            Assert.assertTrue(wereVpsRemovedBySingle);
        }
    }

    @Test
    public void concurrentFireflyInstanceAllMultiPropertyTraversalsCombinedTest() throws InterruptedException {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);
            final GraphTraversalSource masterG = fireflyGraph.traversal();
            final Object id = masterG.addV("foo").next().id();
            final AtomicBoolean isRunning = new AtomicBoolean(true);
            final AtomicReference<Exception> exception = new AtomicReference<>(null);
            final Random rng = new Random();
            final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT * 2);
            for (int i = 0; i < THREAD_COUNT * 2; i++) {
                executorService.submit(() -> {
                    while (isRunning.get()) {
                        final int cardRng = rng.nextInt(10);
                        final int addOrRemoveRng = rng.nextInt(10);
                        final int valueKeyRng = rng.nextInt(5);
                        final int valueTypeRng = rng.nextInt(7);
                        try {
                            final GraphTraversalSource g = fireflyGraph.traversal();
                            if (addOrRemoveRng < 1) {
                                g.V(id).properties(String.valueOf(valueKeyRng)).drop().iterate();
                            } else {
                                final VertexProperty.Cardinality cardinality = cardRng < 1 ?
                                        VertexProperty.Cardinality.single : VertexProperty.Cardinality.list;
                                final Object propertyValue;
                                switch (valueTypeRng) {
                                    case 0:
                                        propertyValue = Long.valueOf(rng.nextInt());
                                        break;
                                    case 1:
                                        propertyValue = rng.nextInt();
                                        break;
                                    case 2:
                                        propertyValue = rng.nextDouble() * 1000;
                                        break;
                                    case 3:
                                        final byte[] byteArr = new byte[8];
                                        rng.nextBytes(byteArr);
                                        propertyValue = byteArr;
                                        break;
                                    case 4:
                                        propertyValue = RandomStringUtils.randomAlphanumeric(10);
                                        break;
                                    case 5:
                                        propertyValue = (rng.nextInt(2) == 0);
                                        break;
                                    default:
                                        propertyValue = null;
                                }
                                g.V(id).property(cardinality, String.valueOf(valueKeyRng), propertyValue).iterate();
                            }
                        } catch (final Exception e) {
                            exception.set(e);
                            break;
                        }
                    }
                });
            }
            final long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < 3500) {
                Thread.sleep(100);
            }
            isRunning.set(false);
            executorService.shutdown();
            Assert.assertTrue(executorService.awaitTermination(1, TimeUnit.SECONDS));
            Assert.assertNull(exception.get());
        }
    }

    private void concurrentFireflyEdgeRemoval(final FireflyGraph fireflyGraph, int threadIdx, final CyclicBarrier gate) {
        try {
            gate.await();
            final GraphTraversalSource g = fireflyGraph.traversal();
            for (int i = 0; i < ADD_REMOVE_COUNT; i++) {
                final byte[] id = getBytesId(i + (long) threadIdx * ADD_REMOVE_COUNT);
                g.E(id).drop().iterate();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void concurrentFireflyEdgeWrite(final FireflyGraph graph, final FireflyVertex vertexA, final FireflyVertex vertexB, final int threadId, final CyclicBarrier gate) {
        try {
            gate.await();
            for (int i = 0; i < ADD_REMOVE_COUNT; i++) {
                final byte[] id = getBytesId(INITIAL_COUNT + i + (long) threadId * ADD_REMOVE_COUNT);
                graph.getAerospikeOperations().writeEdge(graph.getIdFactory().createEdgeId(id), "bar", new ArrayList<>(), vertexA, vertexB);
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
                final FireflyId id = graph.getIdFactory().createVertexPropertyId(actualIdx);
                vertex.property(VertexProperty.Cardinality.single, String.format("%d", actualIdx), actualIdx);
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
                final FireflyId id = graph.getIdFactory().createVertexPropertyId(actualIdx);
                graph.traversal().V().limit(1).properties(String.format("%d", actualIdx)).drop().iterate();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
