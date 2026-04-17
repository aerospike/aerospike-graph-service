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

package com.aerospike.firefly.olap.structure;


import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;

import java.io.Serializable;

public class DistributedReferenceVertexProperty<V> extends ReferenceVertexProperty<V> implements Serializable {
    final ReferenceVertex vertex;
    public DistributedReferenceVertexProperty(final Object id, final Object vertexId) {
        super(id, null, null);
        this.vertex = new ReferenceVertex(vertexId);
    }

    @Override
    public Vertex element() {
        return vertex;
    }

    @Override
    public String toString() {
        return "DistributedReferenceVertexProperty{" +
                "vertex=" + vertex +
                '}';
    }
}
