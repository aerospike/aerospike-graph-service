package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.FireflyEdgeRecord;
import com.aerospike.firefly.structure.id.FireflyEdgeId;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdComposite;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;

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

    public static FireflyEdge create(final FireflyEdgeId edgeId, final FireflyEdgeRecord edgeRecord, final FireflyGraph graph) {
        final String label = edgeRecord.getLabel(edgeId);
        if (label == null) {
            // This means the Edge no longer exists in this record.
            return null;
        }

        final FireflyId outVertex = edgeRecord.getOutV(edgeId);
        final FireflyId inVertex = edgeRecord.getInV(edgeId);

        final Map<String, Object> properties = edgeRecord.getProperties(edgeId);
        final Map<String, Object> typeHints = edgeRecord.getTypeHints(edgeId);

        final boolean isOutSupernode = edgeRecord.getIsOutSupernode(edgeId);
        final boolean isInSupernode = edgeRecord.getIsInSupernode(edgeId);

        final int generation = edgeRecord.getGeneration();

        return create(edgeId, label, graph, outVertex, inVertex, properties, typeHints, isOutSupernode, isInSupernode, generation);
    }
}
