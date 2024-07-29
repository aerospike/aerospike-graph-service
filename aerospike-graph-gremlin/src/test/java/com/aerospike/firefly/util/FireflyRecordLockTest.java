package com.aerospike.firefly.util;

import com.aerospike.client.Key;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.concurrency.FireflyRecordLockHandler;
import org.apache.commons.configuration2.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class FireflyRecordLockTest {
    static private final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);

    private FireflyGraph graph;

    @Before
    public void beforeEach() {
        graph = FireflyGraph.open(CONFIG);
    }

    @After
    public void afterEach() {
        graph.getBaseGraph().dropDatabase(graph, true);
        graph.close();
    }

    @Test
    public void testLockUnlock() throws InterruptedException {
        final FireflyRecordLockHandler handler = graph.getRecordLockHandler();
        final Key key = FireflyRecord.getMergeEdgeKey(graph, 123, 456);
        final AtomicBoolean lockGrabbed = new AtomicBoolean(false);
        final AtomicBoolean locksEqual = new AtomicBoolean(false);

        final FireflyRecordLockHandler.FireflyRecordLock lock1 = handler.getLock(key);
        final Thread second = new Thread(() -> {
            final FireflyRecordLockHandler.FireflyRecordLock lock2 = handler.getLock(key);
            lockGrabbed.set(true);
            locksEqual.set(lock2 == lock1);
            lock2.unlock();
        });
        second.start();
        Thread.sleep(100);
        Assert.assertFalse(lockGrabbed.get());
        lock1.unlock();
        second.join();
        Assert.assertTrue(lockGrabbed.get());
        Assert.assertTrue(locksEqual.get());
    }

    @Test
    public void testDifferentKey() throws InterruptedException {
        final FireflyRecordLockHandler handler = graph.getRecordLockHandler();
        final Key key1 = FireflyRecord.getMergeEdgeKey(graph, 1231, 4561);
        final Key key2 = FireflyRecord.getMergeEdgeKey(graph, 1232, 4572);
        final AtomicBoolean lockGrabbed = new AtomicBoolean(false);
        final AtomicBoolean locksEqual = new AtomicBoolean(true);

        final FireflyRecordLockHandler.FireflyRecordLock lock1 = handler.getLock(key1);
        final Thread second = new Thread(() -> {
            final FireflyRecordLockHandler.FireflyRecordLock lock2 = handler.getLock(key2);
            lockGrabbed.set(true);
            locksEqual.set(lock2 == lock1);
            lock2.unlock();
        });
        second.start();
        second.join();
        Assert.assertTrue(lockGrabbed.get());
        Assert.assertFalse(locksEqual.get());
        lock1.unlock();
    }

    @Test
    public void testLockReleaseWhenNonePending() throws InterruptedException {
        final FireflyRecordLockHandler handler = graph.getRecordLockHandler();
        final Key key = FireflyRecord.getMergeEdgeKey(graph, 1233, 4564);
        System.out.println("Lock1create");
        final FireflyRecordLockHandler.FireflyRecordLock lock1 = handler.getLock(key);
        System.out.println("Lock1created unlock");
        lock1.unlock();
        System.out.println("sleep 100");
        Thread.sleep(100);
        System.out.println("Lock2create");
        final FireflyRecordLockHandler.FireflyRecordLock lock2 = handler.getLock(key);
        System.out.println("Lock2created");
        Assert.assertNotSame(lock1, lock2);
        lock2.unlock();
    }

    @Test
    public void testEvaluationTimeout() throws InterruptedException {
        graph.close();
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.MERGE_EDGE_EVAL_TIMEOUT, "1000");
        graph = FireflyGraph.open(config);
        final FireflyRecordLockHandler handler = graph.getRecordLockHandler();
        final Key key = FireflyRecord.getMergeEdgeKey(graph, 1235, 4566);
        final AtomicBoolean lockGrabbed = new AtomicBoolean(false);
        final AtomicBoolean timeout = new AtomicBoolean(false);

        System.out.println("Lock1create");
        final FireflyRecordLockHandler.FireflyRecordLock lock1 = handler.getLock(key);
        System.out.println("Lock1created");
        final Thread second = new Thread(() -> {
            try {
                System.out.println("Lock2create");
                final FireflyRecordLockHandler.FireflyRecordLock lock2 = handler.getLock(key);
                System.out.println("Lock2created");
                lockGrabbed.set(true);
                System.out.println("grabbed");
                lock2.unlock();
                System.out.println("unlock");
            } catch (final RuntimeException e) {
                System.out.println("lock2 timeout");
                timeout.set(true);
            }
        });
        second.start();
        System.out.println("sleep1start");
        Thread.sleep(500);
        System.out.println("sleep1done");
        Assert.assertFalse(lockGrabbed.get());
        Assert.assertFalse(timeout.get());
        System.out.println("sleep2start");
        Thread.sleep(2000);
        System.out.println("sleep2done");
        Assert.assertFalse(lockGrabbed.get());
        Assert.assertTrue(timeout.get());
        lock1.unlock();
        second.join();
    }

    @Test
    public void testTtl() throws InterruptedException {
        graph.close();
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.MERGE_EDGE_TTL, "1000");
        graph = FireflyGraph.open(config);
        final FireflyRecordLockHandler handler = graph.getRecordLockHandler();
        final Key key = FireflyRecord.getMergeEdgeKey(graph, 1237, 4568);
        final AtomicBoolean lockGrabbed = new AtomicBoolean(false);
        final AtomicBoolean locksEqual = new AtomicBoolean(false);

        System.out.println("Lock1create");
        final FireflyRecordLockHandler.FireflyRecordLock lock1 = handler.getLock(key);
        System.out.println("Lock1created");
        final Thread second = new Thread(() -> {
            System.out.println("Lock2create");
            final FireflyRecordLockHandler.FireflyRecordLock lock2 = handler.getLock(key);
            System.out.println("Lock2created");
            lockGrabbed.set(true);
            System.out.println("grabbed");
            locksEqual.set(lock2 == lock1);
            System.out.println("unlock");
            lock2.unlock();
            System.out.println("unlock done");
        });
        System.out.println("second start");
        second.start();
        System.out.println("sleep1start");
        Thread.sleep(500);
        System.out.println("sleep1done");
        Assert.assertFalse(lockGrabbed.get());
        Thread.sleep(2000);
        System.out.println("sleep2done");
        Assert.assertTrue(lockGrabbed.get());
        Assert.assertTrue(locksEqual.get());
        lock1.unlock();
        System.out.println("unlock1done");
        second.join();
        System.out.println("second join done");
    }

    @Test
    public void testIsFair() throws InterruptedException {
        final FireflyRecordLockHandler handler = graph.getRecordLockHandler();
        final Key key = FireflyRecord.getMergeEdgeKey(graph, 1239, 45610);
        final AtomicInteger orderChecker = new AtomicInteger(0);
        final AtomicBoolean inOrder = new AtomicBoolean(true);
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            final AtomicInteger atomicI = new AtomicInteger(i);
            final Thread thread = new Thread(() -> {
                final FireflyRecordLockHandler.FireflyRecordLock lock = handler.getLock(key);
                if (atomicI.get() != orderChecker.getAndIncrement()) {
                    inOrder.set(false);
                }
                lock.unlock();
            });
            threads.add(i, thread);
        }
        for (int i = 0; i < 32; i++) {
            Thread.sleep(10);
            threads.get(i).start();
        }
        for (final Thread thread : threads) {
            thread.join();
        }
        Assert.assertTrue(inOrder.get());
    }
}
