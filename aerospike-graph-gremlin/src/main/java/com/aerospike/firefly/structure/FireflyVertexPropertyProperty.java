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

package com.aerospike.firefly.structure;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.io.FireflyRecord.getKey;

public class FireflyVertexPropertyProperty<V> extends FireflyProperty<V> {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyVertexPropertyProperty.class);
    private final FireflyGraph graph;
    private final FireflyVertexProperty<?> vertexProperty;

    /**
     * Constructor for FireflyVertexPropertyProperty.
     *
     * @param graph             Graph that property exists in.
     * @param vertexProperty    Vertex Property that property exists on.
     * @param key               Key of property.
     * @param value             Value of property.
     */
    public FireflyVertexPropertyProperty(final FireflyGraph graph, final FireflyVertexProperty<?> vertexProperty,
                                         final String key, final V value) {
        super(vertexProperty, key, value);
        this.graph = graph;
        this.vertexProperty = vertexProperty;
    }

    /**
     * Remove this property from the graph.
     */
    @Override
    public void remove() {
        final AerospikeConnection db = this.graph.getBaseGraph();
        final Key opKey = getKey(db, db.getConfig().vertexAeroSet, ((FireflyVertex) vertexProperty.element()).id);
        final Long schemaVertexPropertyKey = db.schemaManager.getVertexPropertyRead(vertexProperty.key);
        final Long vertexPropertyId = (Long) vertexProperty.id.getStorageId();
        final Long schemaPropertyKey = db.schemaManager.getVpPropertyRead(this.key());

        final Operation removeProperty = MapOperation.removeByKey(db.getConfig().vpPropertyBin, Value.get(schemaPropertyKey),
                MapReturnType.NONE,
                CTX.mapKey(Value.get(schemaVertexPropertyKey)), CTX.mapKey(Value.get(vertexPropertyId)));

        try {
            db.writeOperate(null, opKey, removeProperty);
            vertexProperty.removePropertyFromCache(this.key());
        } catch (final AerospikeGraphException e) {
            if (e.errorCode == ResultCode.OP_NOT_APPLICABLE || e.errorCode == ResultCode.KEY_NOT_FOUND_ERROR) {
                // Special logic to handle when Vertex Property Property has been removed from the Vertex since in this case
                // the key is the Vertex key due to Vertex Properties being packed and thus the key still exists.
                LOG.debug("Ignored exception removing an already-removed vertex property property {}", this, e);
            } else {
                throw e;
            }
        }
    }
}
