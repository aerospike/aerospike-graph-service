package com.aerospike.firefly.structure;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyEdgeId;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.structure.FireflyEdge.PROPERTIES_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.TYPE_HINTS_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.appendRemoveFilterableSupernodePropertyOperation;
import static com.aerospike.firefly.structure.FireflyEdge.isPropertyValuePushdownable;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class FireflyEdgeProperty<V> extends FireflyProperty<V> {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyEdgeProperty.class);
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
    public FireflyEdgeProperty(final FireflyGraph graph, final FireflyEdge edge, final String key, final V value) {
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
        final Value edgeIdMapKey = Value.get(((FireflyEdgeId) edge.id).getEdgeIdBytes());
        final List<Operation> operations = new ArrayList<>();

        final Operation removeProperty = MapOperation.removeByKey(db.EDGE_DATA_BIN, Value.get(key()),
                MapReturnType.NONE, CTX.mapKey(edgeIdMapKey), CTX.listIndex(PROPERTIES_POSITION));
        operations.add(removeProperty);
        final Operation removeTypeHint = MapOperation.removeByKey(db.EDGE_DATA_BIN, Value.get(key()),
                MapReturnType.NONE, CTX.mapKey(edgeIdMapKey), CTX.listIndex(TYPE_HINTS_POSITION));
        operations.add(removeTypeHint);
        try {
            if (isPropertyValuePushdownable(this.value())) {
                appendRemoveFilterableSupernodePropertyOperation(this.edge, this.key(), operations);
            }
        } catch (final NoSuchElementException e) {
            // Do nothing since a property with no value is the same as a value type that can't be pushed down.
        }

        try {
            edge.removePropertyFromCache(key());
            db.writeOperate(null, key, operations.toArray(new Operation[0]));
        } catch (final AerospikeGraphException ae) {
            if (ae.errorCode == ResultCode.OP_NOT_APPLICABLE) {
                // Special logic to handle when Edge has been removed from the Phat Edge since in this case the key is
                // the Phat Edge key and thus the key still exists.
                LOG.debug("Ignored exception removing an already-removed property {}", this, ae);
            } else {
                throw ae;
            }
        }
    }
}
