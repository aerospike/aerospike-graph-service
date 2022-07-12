package com.aerospike.firefly.io.impl.standard;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.Backend;
import com.aerospike.firefly.io.BackendElement;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Vertex  extends BackendElement implements Backend.Vertex {
    private static final Logger logger = LoggerFactory.getLogger(AerospikeConnection.class);

    /**
     * Get a "fast count" of the number of elements in the Vertex set using Aerospike info
     *
     * @return number of Vertices
     */
    public long getVertexCount() {
        return db.getSetSize(db.VERTEX_AERO_SET);
    }
    /**
     * Read a record from VERTEX_AERO_SET and return a constructed FireflyVertex
     *
     * @param graph    Graph handle
     * @param vertexId id of Vertex to read
     * @return Vertex to return
     */

    public FireflyVertex readVertex(final FireflyGraph graph, final FireflyId vertexId) {
        final FireflyRecord fireflyRecord = db.getVertexRecord(vertexId);
        if (fireflyRecord == null) {
            return null;
        }
        return db.vertexFromRecord(graph, fireflyRecord);
    }
    /**
     * write a labeled Vertex record
     *
     * @param graph    handle to Graph instance
     * @param vertexId vertex id to write
     * @param label    vertex label to write
     */
    public void writeVertex(final FireflyGraph graph, final FireflyId vertexId, final String label) {
        logger.debug("Writing Vertex {}.", vertexId.value().toString());
        final Bin labelBin = new Bin(db.LABEL, Value.get(label));
        FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, vertexId, labelBin);
    }

    /**
     * remove a Vertex Record
     *
     * @param graph    refrence to Graph
     * @param vertexId id of Vertex to remove
     */
    public void removeVertex(final FireflyGraph graph, final FireflyId vertexId) {
        logger.debug("Removing Vertex {}.", vertexId.value().toString());
        final Key key = FireflyRecord.getKey(db.namespace, db.VERTEX_AERO_SET, vertexId.toNumericId());
        db.delete(key);
    }

}
