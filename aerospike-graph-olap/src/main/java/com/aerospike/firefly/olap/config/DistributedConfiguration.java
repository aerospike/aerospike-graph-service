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

package com.aerospike.firefly.olap.config;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DistributedConfiguration extends AbstractConfiguration implements Serializable, Iterable {

    private final Map<String, Object> properties = new HashMap<>();

    public DistributedConfiguration() {
        super();
    }

    public DistributedConfiguration(final Configuration configuration) {
        this();
        this.copy(configuration);
    }

    @Override
    protected Iterator<String> getKeysInternal() {
        return properties.keySet().iterator();
    }

    @Override
    protected Object getPropertyInternal(final String s) {
        return properties.get(s);
    }

    @Override
    protected boolean isEmptyInternal() {
        return properties.isEmpty();
    }

    @Override
    protected boolean containsKeyInternal(String s) {
        return properties.containsKey(s);
    }

    @Override
    protected void addPropertyDirect(final String key, final Object value) {
        this.properties.put(key, value);
    }

    @Override
    protected void clearPropertyDirect(final String key) {
        this.properties.remove(key);
    }

    @Override
    public Iterator iterator() {
        return IteratorUtils.map(this.getKeys(), k -> new Pair<>(k, this.getProperty(k)));
    }
}
