package com.aerospike.firefly.olap.structure;

public class AerospikeComputeKey {

    private AerospikeComputeKey() {
    }

    public static String createDouble(final String name, final boolean isVersioned) {
        return name + (isVersioned ? "~d" : "~D");
    }

    public static String createLong(final String name, final boolean isVersioned) {
        return name + (isVersioned ? "~l" : "~L");
    }

    public static String createAccumulator(final String name) {
        return name + "-accumulator";
    }

    public static boolean isVersioned(final String key) {
        return key.endsWith("~d") || key.endsWith("~l");
    }

    public static boolean isDouble(final String key) {
        return key.endsWith("~d") || key.endsWith("~D");
    }

    public static boolean isLong(final String key) {
        return key.endsWith("~l") || key.endsWith("~L");
    }

    public static boolean isAccumulator(final String key) {
        return key.endsWith("-accumulator");
    }
}
