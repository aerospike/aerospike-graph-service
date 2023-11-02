package com.aerospike.firefly.structure;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
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
     * Constructor for RelationalProperty.
     *
     * @param graph   Graph that property exists in.
     * @param vertexProperty Vertex Property that property exists on.
     * @param key     Key of property.
     * @param value   Value of property.
     */
    public FireflyVertexPropertyProperty(final FireflyGraph graph, final FireflyVertexProperty<?> vertexProperty, final String key, final V value) {
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

        final Operation removeProperty = MapOperation.removeByKey(db.PROPERTIES_BIN, Value.get(key()), MapReturnType.NONE,
                CTX.mapKey(Value.get(vertexProperty.id.getStorageId())));
        final Operation removeTypeHint = MapOperation.removeByKey(db.TYPE_HINTS_BIN, Value.get(key()), MapReturnType.NONE,
                CTX.mapKey(Value.get(vertexProperty.id.getStorageId())));

        try {
            vertexProperty.removePropertyFromCache(key());
            db.operate(null, opKey, removeProperty, removeTypeHint);
        } catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when Vertex Property Property has been removed from the Vertex since in this case
                // the key is the Vertex key due to Vertex Properties being packed and thus the key still exists.
                LOG.debug("Ignored exception removing an already-removed vertex property property {}", this, ae);
            } else {
                throw ae;
            }
        }
    }
}
