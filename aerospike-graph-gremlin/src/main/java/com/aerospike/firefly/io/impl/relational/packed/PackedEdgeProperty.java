package com.aerospike.firefly.io.impl.relational.packed;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.io.FireflyRecord.getKey;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class PackedEdgeProperty<V> extends FireflyProperty<V> {
    private static final Logger LOG = LoggerFactory.getLogger(PackedEdgeProperty.class);
    private final FireflyGraph graph;
    private final FireflyEdge edge;

    /**
     * Constructor for RelationalProperty.
     *
     * @param graph   Graph that property exists in.
     * @param edge    Edge that property exists on.
     * @param key     Key of property.
     * @param value   Value of property.
     */
    public PackedEdgeProperty(final FireflyGraph graph, final FireflyEdge edge, final String key, final V value) {
        super(edge, key, value);
        this.graph = graph;
        this.edge = edge;
    }

    /**
     * Remove this property from the graph.
     */
    @Override
    public void remove() {
        final AerospikeConnection db = graph.getBaseGraph();
        final Key key = getKey(db, db.EDGE_AERO_SET, edge.id);
        final Value edgeIdMapKey = Value.get(edge.id.getUserId());

        final Operation removeProperty = MapOperation.removeByKey(db.PROPERTIES_BIN, Value.get(key()),
                MapReturnType.NONE, CTX.mapKey(edgeIdMapKey));
        final Operation removeTypeHint = MapOperation.removeByKey(db.TYPE_HINTS_BIN, Value.get(key()),
                MapReturnType.NONE, CTX.mapKey(edgeIdMapKey));

        try {
            edge.removePropertyFromCache(key());
            db.operate(null, key, removeProperty, removeTypeHint);
        } catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when Edge has been removed from the Phat Edge since in this case the key is
                // the Phat Edge key and thus the key still exists.
                LOG.debug("Ignored exception removing an already-removed property {}", this, ae);
            } else {
                throw ae;
            }
        }
    }
}
