package com.aerospike.firefly.process;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class TestFireflyExceedHeap extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    // 1 kB string.
    private static final int STRING_LENGTH = 1000;
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final Random RANDOM = new Random();
    private static final String RANDOM_STRING;
    static {
        final StringBuilder stringBuilder = new StringBuilder();
        while (stringBuilder.length() < STRING_LENGTH) {
            int idx = (int) (RANDOM.nextFloat() * CHARACTERS.length());
            stringBuilder.append(CHARACTERS.charAt(idx));
        }
        RANDOM_STRING = stringBuilder.toString();
    }

    @Test
    public void testSmallMemoryOverload() {
        graph.close();

        config.setProperty(ConfigurationHelper.Keys.ENABLE_READ_THROUGH_CACHE.toLowerCase(), "false");
        config.setProperty(ConfigurationHelper.Keys.ENABLE_PREFETCH_STRATEGY.toLowerCase(), "false");

        db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);

        final GraphTraversalSource g = graph.traversal();

        // Ensure heap size is ~100 MB. This is done by setting
        // -Djvmheapsize=small when calling maven. For example:
        // mvn test -pl firefly-gremlin -Dtest=TestFireflyExceedHeap -DfailIfNoTests=false -Dintegration.test.properties=packed -Djvmheapsize=small --no-transfer-progress
        long heapSize = Runtime.getRuntime().maxMemory();
        long heapSizeMB = heapSize / 1024 / 1024;
        Assert.assertTrue(heapSizeMB < 220);
        Assert.assertTrue(heapSizeMB > 180);


        // 2000 vertices @ 200 kB per vertex = 400 MB.
        for (int i = 0; i < 2000; i++) {
            if (i != 0 && ((i + 1) % 250) == 0) {
                System.out.println("Adding vertex " + (i + 1));
            }
            GraphTraversal t = g.addV("person");
            // ~200 kB per vertex.
            for (int j = 0; j < 100; j++) {
                t = t.property(RANDOM_STRING + j, RANDOM_STRING + j);
            }
            t.next();
        }

        System.out.println("Heap size: " + heapSizeMB + " MB");
        System.out.println("Available size: " + Runtime.getRuntime().freeMemory() / (1024 * 1024) + " MB");

        // g.V().count().
        g.V().count().next();
    }
}
