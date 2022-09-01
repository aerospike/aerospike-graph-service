package com.aerospike.firefly.io.impl;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.relational.linked.LinkedGraph;
import com.aerospike.firefly.io.impl.relational.linked.LinkedVertexProperty;
import com.aerospike.firefly.io.impl.relational.packed.PackedGraph;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedGraph;
import com.aerospike.firefly.structure.FireflyGraph;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
final public class GraphFactory {
    // We can map the class here but we do not instantiate the Graphs because that would
    // be unnecessary overhead.
    private static final Map<String, Class<? extends FireflyGraph>> DATA_MODEL_MAP = ImmutableMap.of(
            "Linked", LinkedGraph.class,
            "Packed", PackedGraph.class,
            "StarPacked", StarPackedGraph.class
    );
    private static final Logger LOG = LoggerFactory.getLogger(LinkedVertexProperty.class);
    private static final String FIREFLY_DATA_MODEL = "firefly_data_model";
    public static FireflyGraph createGraph(final AerospikeConnection db, final Configuration config) {
        final String dataModel = config.get(String.class, FIREFLY_DATA_MODEL);
        if (!DATA_MODEL_MAP.containsKey(dataModel)) {
            throw new IllegalArgumentException("Unknown graph type: " + config.get(String.class, FIREFLY_DATA_MODEL));
        } else {
            LOG.info("Constructing Graph for {} data model.", dataModel);
            try {
                return DATA_MODEL_MAP.get(dataModel).getConstructor(AerospikeConnection.class, Configuration.class).newInstance(db, config);
            } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
                // This should never happen, but this prevents us from having to put a throws on the function signature.
                // Gotta love Java...
                throw new RuntimeException("Error constructing graph", e);
            }
        }
    }
}
