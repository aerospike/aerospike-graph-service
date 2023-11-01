package com.aerospike.firefly.io.impl.relational;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.server.Settings;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public abstract class RelationalGraph extends FireflyGraph {

    /**
     * Constructor for RelationalGraph.
     *
     * @param db   AerospikeConnection.
     * @param conf Configuration.
     */
    public RelationalGraph(AerospikeConnection db, final Configuration conf, final Settings gremlinServerSettings) {
        super(db, conf, gremlinServerSettings);
    }

}
