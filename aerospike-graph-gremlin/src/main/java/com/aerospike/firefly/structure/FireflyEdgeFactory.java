package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.FireflyEdgeRecord;
import com.aerospike.firefly.structure.id.FireflyEdgeId;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdComposite;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;

import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.structure.FireflyEdge.IN_V_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.IS_IN_SUPERNODE_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.IS_OUT_SUPERNODE_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.LABEL_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.OUT_V_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.PROPERTIES_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.TYPE_HINTS_POSITION;

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

    public static FireflyEdge create(final FireflyEdgeId edgeId, final FireflyEdgeRecord edgeRecord, final FireflyGraph graph) {
        final List<Object> edgeData = edgeRecord.getEdgeData(edgeId);
        if (edgeData == null) {
            // This means the Edge no longer exists in this record.
            return null;
        }

        final String label = (String) edgeData.get(LABEL_POSITION);
        final FireflyId outVertex = (FireflyId) edgeData.get(OUT_V_POSITION);
        final FireflyId inVertex = (FireflyId) edgeData.get(IN_V_POSITION);

        final Map<String, Object> properties = (Map<String, Object>) edgeData.get(PROPERTIES_POSITION);
        final Map<String, Object> typeHints = (Map<String, Object>) edgeData.get(TYPE_HINTS_POSITION);

        final boolean isOutSupernode = (boolean) edgeData.get(IS_OUT_SUPERNODE_POSITION);
        final boolean isInSupernode = (boolean) edgeData.get(IS_IN_SUPERNODE_POSITION);

        final int generation = edgeRecord.getGeneration();

        return create(edgeId, label, graph, outVertex, inVertex, properties, typeHints, isOutSupernode, isInSupernode, generation);
    }
}
