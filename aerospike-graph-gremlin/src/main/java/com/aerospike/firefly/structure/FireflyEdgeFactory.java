/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.FireflyEdgeRecord;
import com.aerospike.firefly.io.aerospike.schema.SchemaManager;
import com.aerospike.firefly.structure.id.FireflyEdgeId;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdComposite;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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

        final SchemaManager schemaManager = graph.getBaseGraph().schemaManager;

        final Long labelDisk = (Long) edgeData.get(LABEL_POSITION);
        final String label = schemaManager.getEdgeLabelString(labelDisk);
        final FireflyId outVertex = (FireflyId) edgeData.get(OUT_V_POSITION);
        final FireflyId inVertex = (FireflyId) edgeData.get(IN_V_POSITION);

        final Map<Long, Object> propertiesDisk = (Map<Long, Object>) edgeData.get(PROPERTIES_POSITION);
        final Map<String, Object> properties = new TreeMap<>();
        schemaManager.populateEdgePropertySchemaMapToStringMap(propertiesDisk, properties);
        final Map<Long, Object> typeHintsDisk = (Map<Long, Object>) edgeData.get(TYPE_HINTS_POSITION);
        final Map<String, Object> typeHints = new HashMap<>();
        schemaManager.populateEdgePropertySchemaMapToStringMap(typeHintsDisk, typeHints);

        final boolean isOutSupernode = (boolean) edgeData.get(IS_OUT_SUPERNODE_POSITION);
        final boolean isInSupernode = (boolean) edgeData.get(IS_IN_SUPERNODE_POSITION);

        final int generation = edgeRecord.getGeneration();

        return create(edgeId, label, graph, outVertex, inVertex, properties, typeHints, isOutSupernode, isInSupernode, generation);
    }
}
