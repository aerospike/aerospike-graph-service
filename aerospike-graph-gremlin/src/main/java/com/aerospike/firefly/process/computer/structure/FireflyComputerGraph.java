package com.aerospike.firefly.process.computer.structure;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.server.Settings;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FireflyComputerGraph extends FireflyGraph {

    // TODO: This will be the new FireflyGraphComputerView. This will ensure that FireflyGraph doesn't suffer any corrupted state issues regarding OLTP/OLAP.
    // TODO: Furthermore, FireflyComputerVertex will hide the complexities of compute properties as in-memory and disk backed vertex data will be all bundled together seemlessly.
    // TODO: Finally, this will set the state better for ResultGraph writebacks.

    public FireflyComputerGraph(final AerospikeConnection db, final Configuration conf, final Settings gremlinServerSettings) {
        super(db, conf, gremlinServerSettings);
    }
}
