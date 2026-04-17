/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class PerfUtil {
    public static class Results {
        public final long min;
        public final long max;
        public final long avg;
        public final Map<Double, Long> percentiles;
        private final long runCount;

        private Results(long runCount, long min, long max, long avg, Map<Double, Long> percentiles) {
            this.runCount = runCount;
            this.min = min;
            this.max = max;
            this.avg = avg;
            this.percentiles = percentiles;
        }

        @Override
        public String toString() {
            return String.format(
                            "\trun count: %d\n" +
                            "\t\tavg:     %f ms\n" +
                            "\t\tmin:     %f ms\n" +
                            "\t\tmax:     %f ms\n" +
                            "\t\tp85:     %f ms\n" +
                            "\t\tp90:     %f ms\n" +
                            "\t\tp99:     %f ms\n" +
                            "\t\tp99.9:   %f ms\n" +
                            "\t\tp99.99:  %f ms\n" +
                            "\t\tp99.999: %f ms\n",
                    runCount,
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
        results.sort(Comparator.reverseOrder());
        final long average = results.stream().reduce(Long::sum).get() / results.size();
        final long max = results.get(0);
        final long min = results.get(results.size() - 1);
        results.sort(Long::compareTo);
        return new Results(runCount, min, max, average, new HashMap<>() {{
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
