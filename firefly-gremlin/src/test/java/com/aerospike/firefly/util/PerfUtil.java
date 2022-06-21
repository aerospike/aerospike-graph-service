package com.aerospike.firefly.util;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.*;
import java.util.stream.IntStream;

import static org.apache.tinkerpop.gremlin.process.traversal.Operator.sum;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class PerfUtil {
    public static class Results {
        public final long min;
        public final long max;
        public final long avg;
        public final Map<Double, Long> percentiles;

        private Results(long min, long max, long avg, Map<Double, Long> percentiles) {
            this.min = min;
            this.max = max;
            this.avg = avg;
            this.percentiles = percentiles;
        }

        @Override
        public String toString() {
            return String.format("avg: %f\n" +
                            "min: %f\n" +
                            "max: %f\n" +
                            "85th: %f\n" +
                            "90th: %f\n" +
                            "99th: %f\n" +
                            "99.9th: %f\n" +
                            "99.99th: %f\n" +
                            "99.999th: %f\n",
                    nsToMs(avg), nsToMs(min), nsToMs(max),
                    nsToMs(percentiles.get(85.)),
                    nsToMs(percentiles.get(90.)),
                    nsToMs(percentiles.get(95.)),
                    nsToMs(percentiles.get(99.)),
                    nsToMs(percentiles.get(99.9)),
                    nsToMs(percentiles.get(99.99)),
                    nsToMs(percentiles.get(99.999)));
        }
    }

    public static long percentile(List<Long> latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
        return latencies.get(index - 1);
    }

    public static double nsToMs(long ns) {
        return ns / 1_000_000.;
    }

    public static Results runTestBatch(int runCount, Runnable test) {
        List<Long> results = new ArrayList<>();
        IntStream.range(0, runCount).forEach(i -> {
            long start = System.nanoTime();
            test.run();
            long finish = System.nanoTime();
            results.add(finish - start);
        });
        results.sort(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o2.compareTo(o1);
            }
        });
        System.out.println(results);
        final long average = results.stream().reduce((l1, l2) -> l1 + l2).get() / results.size();
        final long max = results.get(0);
        final long min = results.get(results.size() - 1);
        results.sort(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o1.compareTo(o2);
            }
        });
        return new Results(min, max, average, new HashMap<>() {{
            put(85., PerfUtil.percentile(results, 85));
            put(90., PerfUtil.percentile(results, 90));
            put(95., PerfUtil.percentile(results, 95));
            put(99., PerfUtil.percentile(results, 99));
            put(99.9, PerfUtil.percentile(results, 99.9));
            put(99.99, PerfUtil.percentile(results, 99.99));
            put(99.999, PerfUtil.percentile(results, 99.999));
        }});
    }
}
