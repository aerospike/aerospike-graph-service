package com.aerospike.firefly.io.query;

import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.query.GraphQuery;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyGraphStepStrategy;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.util.List;
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

        final FireflyGraph graph = mock(FireflyGraph.class);
        setFieldValue(FireflyGraph.class, graph, "fireflyIndexMetadata", new FireflyIndexMetadata(connection));

        final Settings settings = mock(Settings.class);
        setFieldValue(Settings.class, settings, "evaluationTimeout", DEFAULT_TIMEOUT);

        final GraphQuery graphQuery = mock(GraphQuery.class);

        final AtomicReference<Long> timeout = new AtomicReference<>();
        doAnswer((Answer<Void>) invocation -> {
            timeout.set(invocation.getArgument(9));
            return null;
        }).when(graphQuery).scanSet(any(String.class),
                isNull(),
                isNull(),
                any(P.class),
                any(FireflyGraph.TransformKeyRecord.class),
                any(List.class),
                any(Class.class),
                any(boolean.class),
                any(boolean.class),
                any(Long.class));

        when(graph.settings()).thenReturn(settings);
        when(graph.getBaseGraph()).thenReturn(connection);
        setFieldValue(FireflyGraph.class, graph, "graphQuery", graphQuery);

        final GraphTraversalSource g = new GraphTraversalSource(graph);
        final FireflyGraphStepStrategy graphStepStrategy = new FireflyGraphStepStrategy();

        final GraphTraversal traversal = traversalFunc.apply(g);
        graphStepStrategy.apply(traversal.asAdmin());

        try {
            traversal.iterate();
        } catch (final Exception e) {
            // ignore not mocked path
        }

        verify(graphQuery, times(1)).scanSet(any(String.class),
                isNull(),
                isNull(),
                any(P.class),
                any(FireflyGraph.TransformKeyRecord.class),
                any(List.class),
                any(Class.class),
                any(boolean.class),
                any(boolean.class),
                any(Long.class));

        assertNotNull(timeout.get());
        return timeout.get();
    }
}