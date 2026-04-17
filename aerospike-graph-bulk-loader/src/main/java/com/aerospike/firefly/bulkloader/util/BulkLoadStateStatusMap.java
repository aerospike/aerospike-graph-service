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

package com.aerospike.firefly.bulkloader.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_KEY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.ELEMENTS_WRITTEN;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.LOAD_STEP;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.PROGRESS_COMPLETE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.PARTITIONS_COMPLETED_PERCENTAGE;

public class BulkLoadStateStatusMap implements Map<String, Object> {
    private final Map<String, Object> internalMap = new HashMap<>();

    public BulkLoadStateStatusMap(final String stage, final boolean complete, final String status) {
        this(stage, complete, status, null, null);
    }

    public BulkLoadStateStatusMap(final String stage, final boolean complete, final String status,
                                  final Integer percentage, final Long elementsWritten) {
        internalMap.put(LOAD_STEP, stage);
        internalMap.put(PROGRESS_COMPLETE, complete);
        internalMap.put(BULK_LOAD_STATUS_KEY, status);
        if (percentage != null) {
            internalMap.put(PARTITIONS_COMPLETED_PERCENTAGE, percentage);
        }
        if (elementsWritten != null) {
            internalMap.put(ELEMENTS_WRITTEN, elementsWritten);
        }
    }

    @Override
    public int size() {
        return internalMap.size();
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(final Object key) {
        return internalMap.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return internalMap.containsValue(value);
    }

    @Override
    public Object get(final Object key) {
        return internalMap.get(key);
    }

    @Nullable
    @Override
    public Object put(final String key, final Object value) {
        return internalMap.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return internalMap.remove(key);
    }

    @Override
    public void putAll(@NotNull final Map<? extends String, ?> m) {
        internalMap.putAll(m);
    }

    @Override
    public void clear() {
        internalMap.clear();
    }

    @NotNull
    @Override
    public Set<String> keySet() {
        return internalMap.keySet();
    }

    @NotNull
    @Override
    public Collection<Object> values() {
        return internalMap.values();
    }

    @NotNull
    @Override
    public Set<Entry<String, Object>> entrySet() {
        return internalMap.entrySet();
    }

    @Override
    public String toString() {
        return internalMap.toString();
    }
}
