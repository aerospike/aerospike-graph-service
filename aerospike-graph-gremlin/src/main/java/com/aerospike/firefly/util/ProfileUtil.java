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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public final class ProfileUtil {
    private ProfileUtil() {}

    static Map<String, Map<String, AtomicLong>> metrics = new HashMap<>();


    public static void increment(Class<?> clazz, String functionName) {
        Map<String, AtomicLong> clazzMetrics = metrics.getOrDefault(clazz.getName(), new HashMap<>());
        AtomicLong functionMetric = clazzMetrics.getOrDefault(functionName, new AtomicLong());
        functionMetric.addAndGet(1);
        clazzMetrics.put(functionName, functionMetric);
        metrics.put(clazz.getName(), clazzMetrics);
    }

    public static void reset() {
        metrics.clear();
    }

    public static String report() {
        final List<String> results = new ArrayList<>();
        metrics.forEach((clazz, clazzMetrics) -> {
            clazzMetrics.forEach((fn, callCount) -> {
                results.add(String.format("%s.%s: %d calls", clazz, fn, callCount.get()));
            });
        });
        return results.stream().reduce("", (a, b) -> a + "\n" + b);
    }
}
