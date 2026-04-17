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

package com.aerospike.firefly.jsr223;

import com.aerospike.client.AerospikeException;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.process.computer.local.LocalGraphComputer;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.features.FireflyFeatures;
import com.aerospike.firefly.structure.FireflyGraphVariables;
import com.aerospike.firefly.structure.FireflyProperty;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;

public final class FireflyGremlinPlugin extends AbstractGremlinPlugin {
    private static final String NAME = "aerospike.firefly";
    private static final ImportCustomizer imports;

    static {
        try {
            imports = DefaultImportCustomizer.build()
                    .addClassImports(
                            FireflyGraph.class,
                            FireflyFeatures.class,
                            FireflyGraphVariables.class,
                            FireflyElement.class,
                            FireflyEdge.class,
                            FireflyVertex.class,
                            FireflyVertexProperty.class,
                            FireflyProperty.class,
                            FireflyHelper.class,
                            ConfigurationHelper.class,
                            AerospikeException.class,
                            AerospikeConnection.class,
                            LocalGraphComputer.class
                    ).create();
        } catch (Exception ex) {
            System.out.println("ERROR LOADING");
            throw new RuntimeException(ex);
        }
    }

    private static final FireflyGremlinPlugin instance = new FireflyGremlinPlugin();

    public FireflyGremlinPlugin() {
        super(NAME, imports);
    }

    public static GremlinPlugin instance() {
        return instance;
    }

    @Override
    public boolean requireRestart() {
        return true;
    }
}
