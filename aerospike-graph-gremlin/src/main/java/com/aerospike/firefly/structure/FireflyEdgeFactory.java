package com.aerospike.firefly.structure;

import com.aerospike.client.Record;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyEdgeId;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdComposite;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class FireflyEdgeFactory {
    public static FireflyEdge create(final FireflyId fid, final String label, final FireflyGraph graph,
                                     final FireflyId outVertex, final FireflyId inVertex,
                                     final Map<String, Object> properties, final Map<String, Object> typeHints,
                                     final boolean isOutSupernode, final boolean isInSupernode, final int generation) {
        final FireflyPhatEdgeId edgeId;
        if (fid instanceof FireflyIdComposite) {
            edgeId = ((FireflyIdComposite) fid).getEdgeId();
        } else {
            edgeId = (FireflyPhatEdgeId) fid;
        }
        return new FireflyEdge(edgeId, label, graph, outVertex, inVertex, properties, typeHints, isOutSupernode, isInSupernode, generation);
    }

    public static FireflyEdge create(final FireflyId edgeId, final Record record, final FireflyGraph graph) {
        if (record == null) {
            return null;
        }
        final AerospikeConnection db = graph.getBaseGraph();
        final ByteBuffer edgeIdMapKey = ((FireflyEdgeId) edgeId).getEdgeIdBytes();
        final Map<ByteBuffer, List> edgeData = (Map<ByteBuffer, List>) record.getMap(db.EDGE_DATA_BIN);
        // Implicitly assume that if the key is found for label, which is required, then the key exists for the
        // other phat edge maps, since they are all written in the same operate.
        if (!edgeData.containsKey(edgeIdMapKey)) {
            return null;
        }
        final String label = (String) edgeData.get(edgeIdMapKey).get(FireflyEdge.LABEL_POSITION);

        final Object outV = edgeData.get(edgeIdMapKey).get(FireflyEdge.OUT_V_POSITION);
        final FireflyId outVertex = db.getIdFactory().createVertexId(outV);

        final Object inV = edgeData.get(edgeIdMapKey).get(FireflyEdge.IN_V_POSITION);
        final FireflyId inVertex = db.getIdFactory().createVertexId(inV);

        final Map<String, Object> properties = (Map<String, Object>) edgeData.get(edgeIdMapKey).get(FireflyEdge.PROPERTIES_POSITION);
        final Map<String, Object> typeHints = (Map<String, Object>) edgeData.get(edgeIdMapKey).get(FireflyEdge.TYPE_HINTS_POSITION);

        final Map<Long, String> outSupernodes = (Map<Long, String>) record.getMap(graph.getBaseGraph().SUPERNODES_OUT_BIN);
        final Map<Long, String> inSupernodes = (Map<Long, String>) record.getMap(graph.getBaseGraph().SUPERNODES_IN_BIN);
        final boolean isOutSupernode = outSupernodes != null && outSupernodes.containsKey(((FireflyEdgeId) edgeId).getPackingId());
        final boolean isInSupernode = inSupernodes != null && inSupernodes.containsKey(((FireflyEdgeId) edgeId).getPackingId());

        return create(edgeId, label, graph, outVertex, inVertex, properties, typeHints, isOutSupernode, isInSupernode, record.generation);
    }
}
