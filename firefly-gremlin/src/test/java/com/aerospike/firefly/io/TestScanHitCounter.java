package com.aerospike.firefly.io;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestScanHitCounter {
    @Test
    public void testScanHitCounter() {
        AtomicBoolean called = new AtomicBoolean(false);
        ScanHitCounter shc = ScanHitCounter.create(60,2, 3, e -> {
            called.set(true);
            return null;
        });
        shc.increment("a");
        shc.increment("a");
        shc.increment("b");
        shc.increment("c");
        shc.increment("d");
        shc.increment("e");
        assertEquals(2, shc.stats.size());
        assertTrue(shc.stats.asMap().keySet().contains("a"));
        shc.increment("a");
        shc.increment("a");
        assertTrue(called.get());
    }

}
