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
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.FIREFLY_DATA_MODEL;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
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
            } catch (DataModelVersionMismatchException e) {
                if (db.CLEAR_ON_BUILD_ENABLED_FLAG) {
                    LOG.info("Clearing graph...");
                    db.clearNamespace(false);
                } else {
                    throw e;
                }
            }
            db.checkConfigurationCompatibility(config);
            return new FireflyGraph(db, config, getGremlinServerSettings());
        }
    }
}
