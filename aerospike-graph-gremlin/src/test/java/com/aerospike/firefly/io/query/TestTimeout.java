package com.aerospike.firefly.io.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.EventLoops;
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
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestTimeout {
    @Test
    public void testScanTimeout() throws NoSuchFieldException, IllegalAccessException {
        final EventLoops eventLoops = mock(EventLoops.class);
        final IAerospikeClient aerospikeClient = mock(IAerospikeClient.class);

        AtomicReference<ScanPolicy> scanPolicy = new AtomicReference<>();
        doAnswer((Answer<Void>) invocation -> {
            scanPolicy.set(invocation.getArgument(2));
            return null;
        }).when(aerospikeClient).scanPartitions(isNull(),
                anyObject(),
                any(ScanPolicy.class),
                any(PartitionFilter.class),
                isNull(),
                isNull());

        final AerospikeConnection connection = mock(AerospikeConnection.class);
        setFieldValue(AerospikeConnection.class, connection, "QUERY_IMPL", ConfigurationHelper.Keys.QUERY_PAGED);
        setFieldValue(AerospikeConnection.class, connection, "LABEL_BIN", "LABEL_BIN");
        setFieldValue(AerospikeConnection.class, connection, "PAGINATION_PAGE_QUEUE_SIZE", 100);
        setFieldValue(AerospikeConnection.class, connection, "PAGINATION_PAGE_SIZE", 100);
        setFieldValue(AerospikeConnection.class, connection, "eventLoops", eventLoops);

        when(connection.getScanHitCounter()).thenReturn(new ScanHitCounter());
        when(connection.getClient()).thenReturn(aerospikeClient);

        final FireflyGraph graph = mock(FireflyGraph.class);
        setFieldValue(FireflyGraph.class, graph, "fireflyIndexMetadata", new FireflyIndexMetadata(connection));

        final Settings settings = mock(Settings.class);
        setFieldValue(Settings.class, settings, "evaluationTimeout", 12345L);

        when(graph.settings()).thenReturn(settings);
        when(graph.getBaseGraph()).thenReturn(connection);

        final GraphTraversalSource g = new GraphTraversalSource(graph);
        final FireflyGraphStepStrategy graphStepStrategy = new FireflyGraphStepStrategy();
        graphStepStrategy.setSteps(new HashSet<>());

        final GraphTraversal traversal = g.V().has("name", "test");
        graphStepStrategy.apply(traversal.asAdmin());

        try {
            traversal.iterate();
        } catch (final Exception e) {
            // ignore not mocked path
        }

        verify(aerospikeClient, times(1)).scanPartitions(isNull(),
                anyObject(),
                any(ScanPolicy.class),
                any(PartitionFilter.class),
                isNull(),
                isNull());

        assertNotNull(scanPolicy.get());
        assertEquals(12345L, scanPolicy.get().totalTimeout);
    }

    private void setFieldValue(Class clazz, Object object, String fieldName, Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field f = clazz.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(object, value);
    }
}
