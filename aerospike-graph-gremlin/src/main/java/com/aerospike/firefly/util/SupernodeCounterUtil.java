package com.aerospike.firefly.util;

import com.codahale.metrics.Counter;

public class SupernodeCounterUtil {
    private Counter counter = null;

    private static SupernodeCounterUtil instance;

    public static synchronized SupernodeCounterUtil getInstance() {
        if (instance == null) {
            instance = new SupernodeCounterUtil();
        }
        return instance;
    }

    public static void init(final Counter counter) {
        getInstance().counter = counter;
    }

    private SupernodeCounterUtil() {
    }

    public void add() {
        if (counter != null)
            counter.inc();
    }

    public long count() {
        if (counter != null)
            return counter.getCount();
        return 0;
    }
}
