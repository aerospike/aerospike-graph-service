package com.aerospike.firefly.io.impl.relational.linked;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.RelationalGraph;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyReadThroughCacheStrategy;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;

import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

import static com.aerospike.firefly.util.ConfigurationHelper.Keys.KEY_VALUE;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
@Deprecated
final public class LinkedGraph extends RelationalGraph {
    public static final String DATA_MODEL = "linked";

    /**
     * Constructor for LinkedGraph.
     *
     * @param db   AerospikeConnection.
     * @param conf Configuration.
     */
    public LinkedGraph(final AerospikeConnection db, final Configuration conf) {
        super(db, conf);
        synchronized (LinkedGraph.class) {
            TraversalStrategies.GlobalCache.registerStrategies(
                    LinkedGraph.class,
                    TraversalStrategies.GlobalCache.getStrategies(FireflyGraph.class).clone());
        }
    }

    @Override
    protected int getTypeHint() {
        return LinkedVertex.VERTEX_TYPE_HINT;
    }

    @Override
    public String getDataModel() {
        return getDataModelName();
    }

    public static String getDataModelName() {
        return DATA_MODEL;
    }

    /**
     * Write vertex property to Aerospike.
     *
     * @param idValue FireflyId of vertex property to write.
     * @param vertex  Vertex to write property to.
     * @param key     Key of property to write.
     * @param value   Value of property to write.
     * @param <V>     Type of value to write.
     * @return FireflyVertexProperty
     */
    @Override
    public <V> FireflyVertexProperty<V> writeVertexProperty(final FireflyId idValue, final FireflyVertex vertex, final String key, final V value) {
        // Write vertex property to vertex property record first so if we fail we don't end up with null property inside vertex.

        // Write vertex property to Aerospike.
        final FireflyVertexProperty<V> fireflyVertexProperty = LinkedVertexProperty.writeVertexProperty(this, vertex, idValue, key, value);

        // Append vertex property to vertex.
        vertex.writeVertexProperty(fireflyVertexProperty);

        // Return FireflyVertexProperty.
        return fireflyVertexProperty;
    }

    @Override
    public void close() {
        super.close();
    }
}
