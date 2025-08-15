package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Txn;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.io.aerospike.schema.SchemaManager;
import com.aerospike.firefly.runtime.tasks.FireflyGraphSummaryUpdater;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyEdgeId;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdComposite;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import com.aerospike.firefly.structure.transaction.FireflyTransaction;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.MRT_ENABLED_FLAG;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.MRT_TIMEOUT;
import static com.aerospike.firefly.util.ReflectionHelper.setFieldValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestAerospikeOperations {
    private final Path configPath = Path.of("../conf/integration-test-settings-packed.properties");

    @Test
    public void deleteVertex() {
        final Configuration config = ConfigurationHelper.loadFromFile(configPath);
        config.setProperty(MRT_ENABLED_FLAG, "true");
        config.setProperty(MRT_TIMEOUT, 120);

        try (FireflyGraph graph = FireflyGraph.open(config)) {
            graph.getBaseGraph().dropDatabase(graph, false);

            final GraphTraversalSource g = graph.traversal();

            final Vertex v1 = g.addV().next();
            final Vertex v2 = g.addV().next();
            final Edge edge1 = g.addE("l1").from(v1).to(v2).next();
            final Edge edge2 = g.addE("l2").from(v1).to(v2).next();

            g.V(v1.id()).drop().iterate();

            assertEquals(0L, g.E(edge1.id(), edge2.id()).count().next().longValue());
        }
    }

    @Test
    public void testWriteEdgeCommit() {
        final CountDownLatch latchIn = new CountDownLatch(1);
        final CountDownLatch latchOut = new CountDownLatch(1);

        final Configuration config = ConfigurationHelper.loadFromFile(configPath);
        config.setProperty(MRT_ENABLED_FLAG, "true");
        try (FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();
            final FireflyVertex v = (FireflyVertex) g.addV("test").next();

            final FireflyEdgeId edgeId = (FireflyEdgeId) graph.getIdFactory().generateId(graph, FireflyEdge.class);

            // lets start write in new thread, but latch is right before commit
            final Thread thread = new Thread(() -> {
              new FakeAerospikeOperations(graph).writeEdge(edgeId, "l1", List.of(), v, v, latchIn, latchOut);
            });
            thread.start();

            // give some time to write
            latchOut.await(1, TimeUnit.SECONDS);

            // edge should not be commited before latch
            long edges = g.E().hasLabel("l1").count().next();
            assertEquals(0, edges);

            // now commit should do his work
            latchIn.countDown();
            edges = g.E().hasLabel("l1").count().next();
            assertEquals(1, edges);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // in this test there is a latch before the commit
    // to delay the execution and check the functionality of the transactions
    @Test
    public void testEdgeWriteWithTxn() {
        final FireflyGraph graph = mock(FireflyGraph.class);
        setFieldValue(FireflyGraph.class, graph, "fireflySummaryUpdater", mock(FireflyGraphSummaryUpdater.class));
        final FireflyTransaction mockTransaction = mock(FireflyTransaction.class);
        when(mockTransaction.getCurrentTxn()).thenReturn(null);
        setFieldValue(FireflyGraph.class, graph, "transaction", mockTransaction);

        final FireflyId inVertexId = mock(FireflyId.class);
        when(inVertexId.getKeyHashString()).thenReturn("inId");
        final FireflyVertex inVertex = new FireflyVertex(inVertexId, "label", graph, new HashMap<>(), new HashMap<>(),
                Map.of(), Map.of(), Map.of(), false);

        final FireflyId outVertexId = mock(FireflyId.class);
        when(outVertexId.getKeyHashString()).thenReturn("outId");
        final FireflyVertex outVertex = new FireflyVertex(outVertexId, "label", graph, new HashMap<>(), new HashMap<>(),
                Map.of(), Map.of(), Map.of(), false);

        final FireflyPhatEdgeId edgeId = mock(FireflyPhatEdgeId.class);
        final FireflyIdComposite compositeIdIn = mock(FireflyIdComposite.class);
        final FireflyIdComposite compositeIdOut = mock(FireflyIdComposite.class);

        final SchemaManager schemaManager = mock(SchemaManager.class);
        doNothing().when(schemaManager).populateEdgePropertyStringMapToSchemaMap(any(), any());
        when(schemaManager.getEdgeLabelWrite(any())).thenReturn(0L);

        final AerospikeConnection connection = mock(AerospikeConnection.class);
        setFieldValue(AerospikeConnection.class, connection, "EDGE_CACHE_DISABLED_BIN", "EDGE_CACHE_DISABLED_BIN");
        setFieldValue(AerospikeConnection.class, connection, "MRT_ENABLED", true);
        setFieldValue(AerospikeConnection.class, connection, "MRT_TIMEOUT", 1234);
        setFieldValue(AerospikeConnection.class, connection, "schemaManager", schemaManager);
        final FireflyIdFactory fireflyIdFactory = mock(FireflyIdFactory.class);
        when(fireflyIdFactory.createCompositeEdgeId(edgeId, inVertexId)).thenReturn(compositeIdIn);
        when(fireflyIdFactory.createCompositeEdgeId(edgeId, outVertexId)).thenReturn(compositeIdOut);
        when(fireflyIdFactory.generateId(graph, FireflyEdge.class)).thenReturn(edgeId);
        when(connection.writeOperate(any(WritePolicy.class), any(Key.class),
                any(Operation.class)))
                .thenReturn(null);

        final List<WritePolicy> writePolicy = new ArrayList<>();
        doAnswer(invocation -> {
            writePolicy.add(invocation.getArgument(0));
            return new Record(Map.of("EDGE_CACHE_DISABLED_BIN", List.of(true, false)), 0, 0);
        }).when(connection).writeOperate(any(WritePolicy.class), any(Key.class), any(Set.class), any(Boolean.class), any(Operation.class));
        doAnswer(invocation -> {
            writePolicy.add(invocation.getArgument(0));
            return new Record(Map.of("EDGE_CACHE_DISABLED_BIN", List.of(true, false)), 0, 0);
        }).when(connection).writeOperate(any(WritePolicy.class), any(Key.class),any(Operation.class));

        final List<Txn> txn = new ArrayList<>();
        doAnswer(invocation -> {
            txn.add(invocation.getArgument(0));
            return null;
        }).when(connection).commit(any(Txn.class));

        when(graph.getBaseGraph()).thenReturn(connection);
        when(graph.getIdFactory()).thenReturn(fireflyIdFactory);
        when(graph.tx()).thenReturn(mockTransaction);

        final AerospikeOperations operations = new AerospikeOperations(graph);

        operations.writeEdge("test", List.of(), inVertex, outVertex);

        // should commit txn only once
        assertNotNull(txn.get(0));
        assertEquals(1, txn.size());
        // 2 writes for vertices, and 1 for edge
        assertEquals(3, writePolicy.size());
        // all should have same txn
        writePolicy.forEach(p -> assertEquals(txn.get(0), p.txn));
        // should use correct timeout
        assertEquals(1234, txn.get(0).getTimeout());
    }

    public static class FakeAerospikeOperations extends AerospikeOperations {

        public FakeAerospikeOperations(final FireflyGraph graph) {
            super(graph);
        }

        public FireflyEdge writeEdge(final FireflyEdgeId edgeId,
                                     final String label,
                                     final List<Map.Entry<String, Object>> properties,
                                     final FireflyVertex inVertex,
                                     final FireflyVertex outVertex,
                                     final CountDownLatch latchIn,
                                     final CountDownLatch latchOut) {
            final Txn txn = getOrCreateTxn();

            try {
                // Write edge to vertex, if edge write fails, null check on edge record will protect from inconsistent data.
                // Add edge to inVertex and outVertex.
                final boolean inVertexCacheWrite = writeEdgeToVertex(inVertex, Direction.IN, graph.getIdFactory().createCompositeEdgeId(edgeId, outVertex.id), label, txn);
                final boolean outVertexCacheWrite = writeEdgeToVertex(outVertex, Direction.OUT, graph.getIdFactory().createCompositeEdgeId(edgeId, inVertex.id), label, txn);

                // Write edge to Aerospike and return FireflyEdge.
                final FireflyEdge edge = writeEdgeToRecord(edgeId, label, properties, inVertex, outVertex, inVertexCacheWrite, outVertexCacheWrite, txn);

                latchOut.countDown();
                latchIn.await(1, TimeUnit.SECONDS);
                db.commit(txn);

                return edge;
            } catch (final RuntimeException e) {
                db.rollback(txn);
                throw e;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
