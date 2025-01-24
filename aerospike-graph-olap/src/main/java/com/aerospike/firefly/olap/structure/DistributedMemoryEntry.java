package com.aerospike.firefly.olap.structure;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import java.io.Serializable;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.Objects;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * Most of the Distributed* classes are adapted from the Spark* in TinkerPop from by Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DistributedMemoryEntry<R> implements Serializable {
    private R value;

    public DistributedMemoryEntry() {

    }

    public DistributedMemoryEntry(final R value) {
        this.value = value;
    }

    // Getters and Setters

    public static Encoder<DistributedMemoryEntry> getEncoder() {
        return Encoders.bean(DistributedMemoryEntry.class);
    }

    public boolean isEmpty() {
        return null == this.value;
    }

    public static <R> DistributedMemoryEntry<R> empty() {
        return new DistributedMemoryEntry<>(null);
    }

    public R get() {
        return value;
    }

    public void set(final R value) {
        this.value = value;
    }
    /**
     * Shrink the content to display for both Collection and Map
     * @return
     */
    private String shrinkedToString() {
        int s = 0;
        // handle Collection and Map
        if (this.value instanceof Collection) {
            final Collection c = (Collection)value;
            s = c.size();
        } else if (this.value instanceof Map) {
            final Map m = (Map)value;
            s = m.size();
        }
        if (s > 0) {
            final StringBuilder sb = new StringBuilder();
            sb.append(this.value.getClass().toString()).append("@size=").append(s);
            return sb.toString();
        }
        // fallback
        return Objects.toString(this.value);
    }

    @Override
    public String toString() {
        // Spark's background logging apparently tries to log a `toString()` of certain objects while they're being
        // modified, which then throws a ConcurrentModificationException. We probably can't make any arbitrary object
        // thread-safe, but we can easily retry on such cases and eventually we should always get a result.
        final int maxAttempts = 5;
        for (int i = maxAttempts; ; ) {
            try {
                return shrinkedToString();
            } catch (ConcurrentModificationException cme) {
                if (--i > 0) {
                    System.out.println(String.format("Failed to toString() object held by ObjectWritable, retrying %d more %s.",
                            i, i == 1 ? "time" : "times"));
                } else break;
                if (i < maxAttempts - 1) {
                    try {
                        Thread.sleep((maxAttempts - i - 1) * 100);
                    } catch (InterruptedException ignored) {
                        break;
                    }
                }
            }
        }
        return this.value.getClass().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DistributedMemoryEntry<?> that = (DistributedMemoryEntry<?>) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
