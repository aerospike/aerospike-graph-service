package com.aerospike.graph.api;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AerospikeGraphApiBuilder {
    private Configuration configuration = null;
    private final Map<String, String> configurationMap;

    AerospikeGraphApiBuilder() {
        this.configurationMap = new ConcurrentHashMap<>();
    }

    public AerospikeGraphApiBuilder withConfiguration(final Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public AerospikeGraphApiBuilder withConfiguration(final String filePath) {
        this.configuration = ConfigurationHelper.loadFromFile(filePath);
        return this;
    }

    public AerospikeGraphApiBuilder withConfiguration(final Path path) {
        this.configuration = ConfigurationHelper.loadFromFile(path);
        return this;
    }

    public AerospikeGraphApiBuilder withHost(final String host) {
        this.configurationMap.put(ConfigurationHelper.Keys.AEROSPIKE_HOST, host);
        return this;
    }

    public AerospikeGraphApiBuilder withPort(final int port) {
        this.configurationMap.put(ConfigurationHelper.Keys.AEROSPIKE_PORT, String.valueOf(port));
        return this;
    }

    public AerospikeGraphApiBuilder withNamespace(final String namespace) {
        this.configurationMap.put(ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE, namespace);
        return this;
    }

    public AerospikeGraphApiBuilder withProperty(final String property, final String value) {
        this.configurationMap.put(property.toLowerCase(), value);
        return this;
    }

    public AerospikeGraphApi build() {
        if (this.configuration == null) {
            this.configuration = new MapConfiguration(configurationMap);
        } else {
            for (final Map.Entry<String, String> config : configurationMap.entrySet()) {
                this.configuration.setProperty(config.getKey(), config.getValue());
            }
        }

        if (this.configuration.containsKey(ConfigurationHelper.Keys.AEROSPIKE_HOST) &&
                this.configuration.containsKey(ConfigurationHelper.Keys.AEROSPIKE_PORT) &&
                this.configuration.containsKey(ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE)) {
            return new AerospikeGraph(FireflyGraph.open(this.configuration));
        } else {
            throw new IllegalArgumentException("Host, Port, and Namespace must be provided to instantiate an AerospikeGraphApi.");
        }
    }
}
