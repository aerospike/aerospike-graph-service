package com.aerospike.firefly.util;

import com.codahale.metrics.Counter;

public class SupernodesTraversedCounterUtil {
    private Counter counter = null;

    private static SupernodesTraversedCounterUtil instance;

    public static synchronized SupernodesTraversedCounterUtil getInstance() {
        if (instance == null) {
            instance = new SupernodesTraversedCounterUtil();
        }
        return instance;
    }

    public static void init(final Counter counter) {
        getInstance().counter = counter;
    }

    private SupernodesTraversedCounterUtil() {
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
