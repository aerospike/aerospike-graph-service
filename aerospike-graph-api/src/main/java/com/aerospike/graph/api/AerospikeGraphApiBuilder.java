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

package com.aerospike.graph.api;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fluent builder for {@link AerospikeGraphApi} instances.
 *
 * <p>A builder is obtained via {@link AerospikeGraphApi#builder()}. Callers
 * supply cluster coordinates either piecemeal through the
 * {@code withHost} / {@code withPort} / {@code withNamespace} methods or
 * wholesale through one of the {@link #withConfiguration(Configuration)
 * withConfiguration} overloads. Additional graph-engine options may be
 * supplied via {@link #withProperty(String, String)}.
 *
 * <p>When both an externally supplied configuration and individual
 * properties are provided, the individual properties <strong>override</strong>
 * the same keys in the supplied configuration.
 *
 * <p>At minimum, {@link #build()} requires that the host, port, and
 * namespace keys be present (either via explicit setters or a configuration
 * source) and will throw an {@link IllegalArgumentException} otherwise.
 *
 * <p>Builders are not intended for concurrent use; each call chain should
 * be single-threaded.
 */
public class AerospikeGraphApiBuilder {
    private Configuration configuration = null;
    private final Map<String, String> configurationMap;

    AerospikeGraphApiBuilder() {
        this.configurationMap = new ConcurrentHashMap<>();
    }

    /**
     * Seeds the builder with a pre-populated Apache Commons
     * {@link Configuration}. Any individual properties set via subsequent
     * {@code with*} calls will override matching keys in this configuration
     * at {@link #build()} time.
     *
     * @param configuration the configuration to start from; must not be {@code null}
     * @return this builder, for chaining
     */
    public AerospikeGraphApiBuilder withConfiguration(final Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    /**
     * Loads a configuration from the given file system path (as a
     * {@link String}). Supported formats are those recognised by
     * {@code ConfigurationHelper} (properties and YAML).
     *
     * @param filePath path to the configuration file; must be readable
     * @return this builder, for chaining
     */
    public AerospikeGraphApiBuilder withConfiguration(final String filePath) {
        this.configuration = ConfigurationHelper.loadFromFile(filePath);
        return this;
    }

    /**
     * Loads a configuration from the given {@link Path}. Equivalent to
     * {@link #withConfiguration(String)} but avoids the {@code String}
     * conversion.
     *
     * @param path path to the configuration file; must be readable
     * @return this builder, for chaining
     */
    public AerospikeGraphApiBuilder withConfiguration(final Path path) {
        this.configuration = ConfigurationHelper.loadFromFile(path);
        return this;
    }

    /**
     * Sets the Aerospike seed host that the graph service should connect
     * to. Required unless supplied via
     * {@link #withConfiguration(Configuration) withConfiguration}.
     *
     * @param host hostname or IP address of an Aerospike seed node
     * @return this builder, for chaining
     */
    public AerospikeGraphApiBuilder withHost(final String host) {
        this.configurationMap.put(ConfigurationHelper.Keys.AEROSPIKE_HOST, host);
        return this;
    }

    /**
     * Sets the Aerospike service port (typically {@code 3000}).
     *
     * @param port TCP port of the Aerospike service
     * @return this builder, for chaining
     */
    public AerospikeGraphApiBuilder withPort(final int port) {
        this.configurationMap.put(ConfigurationHelper.Keys.AEROSPIKE_PORT, String.valueOf(port));
        return this;
    }

    /**
     * Sets the Aerospike namespace that backs the graph. The namespace must
     * already exist in the cluster configuration.
     *
     * @param namespace Aerospike namespace name
     * @return this builder, for chaining
     */
    public AerospikeGraphApiBuilder withNamespace(final String namespace) {
        this.configurationMap.put(ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE, namespace);
        return this;
    }

    /**
     * Sets an arbitrary graph-engine property. Property keys are normalised
     * to lower-case before being stored. Anything not covered by the
     * dedicated setters above (for example read policies, batch sizes, or
     * index tuning) is exposed this way.
     *
     * @param property property key (case-insensitive)
     * @param value    property value as a string
     * @return this builder, for chaining
     */
    public AerospikeGraphApiBuilder withProperty(final String property, final String value) {
        this.configurationMap.put(property.toLowerCase(), value);
        return this;
    }

    /**
     * Builds and returns a configured {@link AerospikeGraphApi} instance.
     *
     * <p>The returned instance owns a connection to the Aerospike cluster
     * and <strong>must</strong> be closed (via try-with-resources or an
     * explicit {@link AerospikeGraphApi#close() close()}) when no longer
     * needed.
     *
     * @return a configured, open graph instance
     * @throws IllegalArgumentException if the effective configuration is
     *         missing any of the required keys (host, port, namespace)
     */
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
