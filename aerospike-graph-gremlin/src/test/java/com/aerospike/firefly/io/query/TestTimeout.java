package com.aerospike.firefly.io.query;

import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.ScanHitCounter;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyGraphStepStrategy;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestTimeout {
    private static final long DEFAULT_TIMEOUT = 12345;

    @Test
    public void testScanTimeout() throws NoSuchFieldException, IllegalAccessException {
        final long timeout = getUsedTimeoutFromScan((g) -> g.V().has("name", "test"));
        assertEquals(DEFAULT_TIMEOUT, timeout);
    }

    @Test
    public void testScanTimeoutWithOverride() throws NoSuchFieldException, IllegalAccessException {
        final long timeout = getUsedTimeoutFromScan((g) -> g.with("evaluationTimeout", 321L).V().has("name", "test"));
        assertEquals(321L, timeout);
    }

    private void setFieldValue(Class clazz, Object object, String fieldName, Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field f = clazz.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(object, value);
    }

    private long getUsedTimeoutFromScan(Function<GraphTraversalSource, GraphTraversal> traversalFunc) throws NoSuchFieldException, IllegalAccessException {
        final AerospikeConnection connection = mock(AerospikeConnection.class);
        setFieldValue(AerospikeConnection.class, connection, "QUERY_IMPL", ConfigurationHelper.Keys.QUERY_PAGED);
        setFieldValue(AerospikeConnection.class, connection, "LABEL_BIN", "LABEL_BIN");
        setFieldValue(AerospikeConnection.class, connection, "PAGINATION_PAGE_QUEUE_SIZE", 100);

        when(connection.getScanHitCounter()).thenReturn(new ScanHitCounter());

        final AtomicReference<ScanPolicy> scanPolicy = new AtomicReference<>();
        doAnswer((Answer<Void>) invocation -> {
            scanPolicy.set(invocation.getArgument(1));
            return null;
        }).when(connection).scanPartitions(any(RecordSequenceListener.class),
                any(ScanPolicy.class),
                any(PartitionFilter.class),
                isNull());

        final FireflyGraph graph = mock(FireflyGraph.class);
        setFieldValue(FireflyGraph.class, graph, "fireflyIndexMetadata", new FireflyIndexMetadata(connection));

        final Settings settings = mock(Settings.class);
        setFieldValue(Settings.class, settings, "evaluationTimeout", DEFAULT_TIMEOUT);

        when(graph.settings()).thenReturn(settings);
        when(graph.getBaseGraph()).thenReturn(connection);

        final GraphTraversalSource g = new GraphTraversalSource(graph);
        final FireflyGraphStepStrategy graphStepStrategy = new FireflyGraphStepStrategy();

        final GraphTraversal traversal = traversalFunc.apply(g);
        graphStepStrategy.apply(traversal.asAdmin());

        try {
            traversal.iterate();
        } catch (final Exception e) {
            // ignore not mocked path
        }

        verify(connection, times(1)).scanPartitions(any(RecordSequenceListener.class),
                any(ScanPolicy.class),
                any(PartitionFilter.class),
                isNull());

        assertNotNull(scanPolicy.get());
        return scanPolicy.get().totalTimeout;
    }
}