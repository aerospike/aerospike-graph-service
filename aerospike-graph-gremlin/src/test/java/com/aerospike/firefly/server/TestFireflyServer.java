package com.aerospike.firefly.server;

import com.aerospike.firefly.runtime.FireflyServer;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class TestFireflyServer {

    private static final long SECOND = 1000;
    private static final long MINUTE = 60 * SECOND;
    private static final long TEST_DURATION = 10 * MINUTE;
    private long totalExecuted = 0;
    private FireflyServer server;
    private final boolean failed = false;
    private final List<Long> heapSizes = new ArrayList<>();

    @Test
    public void testServer() throws Exception {
        server = FireflyServer.main(new String[] {"../conf/firefly-gremlin-server-local.yaml"});
        HEAP_REPORTING_TIMER.schedule(new HeapReport(), 0, 5000);
        runManyQueries();
        uploadHeapUsage();
    }

    private static final Timer HEAP_REPORTING_TIMER = new Timer(true);

    private class HeapReport extends TimerTask {
        @Override
        public void run() {
            System.gc();
            System.gc();
            System.gc();
            heapSizes.add(Runtime.getRuntime().totalMemory() / 1024 / 1024);
        }
    }

    public void runManyQueries() throws Exception {
        final long startTime = System.currentTimeMillis();
        final Cluster cluster = Cluster.build().addContactPoint("localhost").port(8182).create();
        final DriverRemoteConnection drc = DriverRemoteConnection.using(cluster, "g");
        final GraphTraversalSource g = traversal().withRemote(drc);
        final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        g.V().drop().iterate();
        for (int i = 0; i < 25000; i++) {
            g.addV("randomVertex").
                    property("last_seen", String.format("%d", System.currentTimeMillis())).
                    property("something", String.format("%d", i)).
                    property("otherThing", String.format("%d", i)).
                    property("anotherThing", String.format("%d", i)).
                    property("yetAnotherThing", String.format("%d", i)).
                    property("andAnotherThing", String.format("%d", i)).
                    property("andYetAnotherThing", String.format("%d", i)).
                    iterate();
        }

        final List<Object> ids = g.V().id().toList();
        final List<Future<?>> futures = new ArrayList<>();
        while (System.currentTimeMillis() - startTime <= TEST_DURATION) {
            while (futures.size() > 500) {
                Thread.sleep(100);
                final List<Future<?>> completedFutures = futures.stream().filter(Future::isDone).collect(Collectors.toList());
                completedFutures.forEach(f -> {
                    try {
                        f.get();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                totalExecuted += completedFutures.size();
                futures.removeAll(completedFutures);
            }
            final Random rand = new Random();
            final List<Object> idz = new ArrayList<>();
            for (int i = 0; i < 9; i++) {
                idz.add(ids.get(rand.nextInt(ids.size())));
            }
            if (rand.nextBoolean()) {
                futures.add(executorService.submit(() -> g.V(idz).properties("last_seen").toList()));
            } else {
                futures.add(executorService.submit(() -> g.V(idz).property("last_seen", System.currentTimeMillis()).iterate()));
            }
        }
        futures.forEach(f-> {
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        drc.close();
        Thread.sleep(30000);
    }

    void uploadHeapUsage() {
        final List<Long> trimmedHeap = heapSizes.subList(heapSizes.size() - 20, heapSizes.size());

        // Allow 50 MB swings in heap before we investigate deeper.
        final Long averageHeap = trimmedHeap.stream().mapToLong(Long::longValue).sum() / trimmedHeap.size();
        final Long maxHeap = trimmedHeap.stream().mapToLong(Long::longValue).max().getAsLong();
        final Long minHeap = trimmedHeap.stream().mapToLong(Long::longValue).min().getAsLong();


        final JSONArray root = new JSONArray();
        JSONObject obj = new JSONObject();
        obj.put("name", "Heap Usage Average");
        obj.put("unit", "MB");
        obj.put("value", averageHeap);
        obj.put("range", maxHeap - minHeap);
        root.put(obj);
        obj = new JSONObject();
        obj.put("name", "Heap Usage Max");
        obj.put("unit", "MB");
        obj.put("value", maxHeap);
        obj.put("range", maxHeap - minHeap);
        root.put(obj);
        obj = new JSONObject();
        obj.put("name", "Heap Usage Min");
        obj.put("unit", "MB");
        obj.put("value", minHeap);
        obj.put("range", maxHeap - minHeap);
        root.put(obj);
        try (final FileWriter file = new FileWriter("target/jmh-result.json")) {
            file.write(root.toString());
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
}
