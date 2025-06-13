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

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
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
        final Key opKey = getKey(db, db.VERTEX_AERO_SET, ((FireflyVertex) vertexProperty.element()).id);
        final Long schemaVertexPropertyKey = db.schemaManager.getVertexPropertyRead(vertexProperty.key);
        final Long vertexPropertyId = (Long) vertexProperty.id.getStorageId();
        final Long schemaPropertyKey = db.schemaManager.getVpPropertyRead(this.key());

        final Operation removeProperty = MapOperation.removeByKey(db.VP_PROPERTY_BIN, Value.get(schemaPropertyKey),
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
