package com.aerospike.firefly.io.utils;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class TestBloomFilterIdCache {
    // Note, this test relies on probability, which could cause errors if ID_COUNT is set to a large number.
    private AerospikeConnection db;
    private static final int ID_COUNT = 100;
    private static final int THREAD_COUNT = 8;
    private FireflyGraph graph;

    @Before
    public void setup() {
        db = AerospikeConnection.connect(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES));
        graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES));
        graph.getBaseGraph().dropDatabase(graph, false);
    }

    @After
    public void cleanup() {
        db.dropDatabase(graph, false);
        db.close();
    }

    @Test
    public void testSingleBloomFilter() throws IOException {
        // Create id set.
        Set<Long> ids = new HashSet<>();

        // Grab ids from bloom filter and ensure we have no duplicates.
        for (int i = 0; i < ID_COUNT; i++) {
            if (BloomFilterIdCache.takeIdIfAvailable(db, db.getNamespace(), "VERTEX_TEST_ID", i)) {
                ids.add((long) i);
            }
        }
        assertEquals(ID_COUNT, ids.size());
    }

    @Test
    public void testMultiBloomFilterNoCollision() throws IOException, InterruptedException {
        // Create id set and ExecutorService.
        Set<Long> ids = new HashSet<>();
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        // Generate list of tasks.
        List<Callable<Void>> callableTasks = new ArrayList<>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            callableTasks.add(new InsertBloomFilter(i, ids));
        }

        // Run all tasks and wait for completion.
        List<Future<Void>> futures = executor.invokeAll(callableTasks);
        futures.forEach(f -> {
            try {
                f.get();
            } catch (Exception ignored) {
            }
        });

        // Check number of ids.
        assertEquals(ID_COUNT * THREAD_COUNT, ids.size());
    }

    @Test
    public void testMultiBloomFilterWithCollision() throws InterruptedException {
        // Create id set and ExecutorService.
        Set<Long> ids = new HashSet<>();
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        // Generate list of tasks.
        List<Callable<Void>> callableTasks = new ArrayList<>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            callableTasks.add(new InsertBloomFilter(i / 2, ids));
        }

        // Run all tasks and wait for completion.
        List<Future<Void>> futures = executor.invokeAll(callableTasks);
        futures.forEach(f -> {
            try {
                f.get();
            } catch (Exception ignored) {
            }
        });

        // Check number of ids.
        assertEquals(ID_COUNT * THREAD_COUNT / 2, ids.size());
    }

    public class InsertBloomFilter implements Callable<Void> {
        private final int index;
        private final Set<Long> ids;

        public InsertBloomFilter(int index, Set<Long> ids) {
            this.index = index;
            this.ids = ids;
        }

        public Void call() throws IOException {
            for (int i = index * 100; i < (index + 1) * 100; i++) {
                if (BloomFilterIdCache.takeIdIfAvailable(db, db.getNamespace(), "VERTEX_TEST_ID", i)) {
                    ids.add((long) i);
                }
            }
            return null;
        }
    }
}
