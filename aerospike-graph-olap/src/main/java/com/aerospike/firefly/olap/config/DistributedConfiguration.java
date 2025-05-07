package com.aerospike.firefly.olap.config;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
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
