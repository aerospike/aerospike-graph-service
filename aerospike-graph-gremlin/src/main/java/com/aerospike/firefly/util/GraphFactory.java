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

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.DataModelVersioning;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.config.FireflyConfiguration;
import com.aerospike.firefly.util.exceptions.DataModelVersionMismatchException;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.aerospike.firefly.structure.FireflyGraph.getGremlinServerSettings;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.CLEAR_ON_VERSION_INCOMPATIBILITY;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.FIREFLY_DATA_MODEL;

final public class GraphFactory {
    private static final Map<String, Class<? extends FireflyGraph>> DATA_MODEL_MAP = ImmutableMap.of(
            FireflyGraph.DATA_MODEL, FireflyGraph.class
    );
    private static final Logger LOG = LoggerFactory.getLogger(GraphFactory.class);

    public static FireflyGraph createGraph(final AerospikeConnection db, final FireflyConfiguration config) {
        final String dataModel = ConfigurationHelper.getOrDefaultString(FIREFLY_DATA_MODEL, config);
        if (!DATA_MODEL_MAP.containsKey(dataModel)) {
            throw new IllegalArgumentException("Unknown graph type: " + dataModel);
        } else {
            LOG.info("Constructing Graph for {} data model.", dataModel);
            try {
                DataModelVersioning.checkVersionCompatibility(db);
            } catch (final DataModelVersionMismatchException e) {
                if (ConfigurationHelper.getOrDefaultBool(CLEAR_ON_VERSION_INCOMPATIBILITY, config)) {
                    LOG.warn("Data model version mismatch detected. Clearing the graph.");
                    db.dropDatabase(null, true);
                } else {
                    throw e;
                }
            }
            db.checkConfigurationCompatibility(config);
            return new FireflyGraph(db, config, getGremlinServerSettings());
        }
    }
}
