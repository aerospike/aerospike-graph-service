package com.aerospike.firefly.runtime;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestWaitsForSindexComplete {
    private static final int THREAD_COUNT = 8;
    private static final int STRING_LENGTH = 250;
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final Random RANDOM = new Random();
    private static final int INSERT_COUNT = 100000;

    @Test
    public void testWaitsForSindexComplete() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            // Drop graph.
            graph.getBaseGraph().dropGraphIndices(graph);
            graph.traversal().V().drop().iterate();

            // Load graph.
            final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
            for (int i = 0; i < THREAD_COUNT; i++) {
                executorService.submit(() -> {
                    for (int j = 0; j < INSERT_COUNT; j++) {
                        final StringBuilder stringBuilder = new StringBuilder();
                        while (stringBuilder.length() < STRING_LENGTH) {
                            int idx = (int) (RANDOM.nextFloat() * CHARACTERS.length());
                            stringBuilder.append(CHARACTERS.charAt(idx));
                        }
                        graph.traversal().addV("vertex").
                                property("str1", stringBuilder.toString()).
                                property("str2", stringBuilder.toString()).
                                property("str3", stringBuilder.toString()).
                                property("str4", stringBuilder.toString()).
                                property("str5", stringBuilder.toString()).
                                iterate();
                    }
                });
            }

            executorService.shutdown();
            boolean completed = executorService.awaitTermination(2, TimeUnit.MINUTES);
            if (!completed) {
                throw new RuntimeException("Failed to complete within 2 minutes");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Create sindex in background on the property and quickly query it.
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES, "str1,str2,str3,str4,str5");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            long count = graph.traversal().V().has("str5", "barazfvsad").count().next();

            // Test is only valid if sindex is not ready yet.
            final Map<String, Long> info = (Map<String, Long>) graph.traversal().call("aerospike.graph.admin.index.status").
                    with("property_key", "str5").
                    with("element_type", "vertex").next();
            Assert.assertTrue(info.get("percent_complete") < 100L);
        }
    }
}
