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

package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;

public class LazyVertexPropertyIdTransform extends LazyIdTransform {

    protected LazyVertexPropertyIdTransform(final Object objectId, final FireflyGraph graph) {
        super(objectId, graph);
    }

    @Override
    public FireflyId transform() {
        if (this.id == null) {
            this.id = this.graph.getIdFactory().createVertexPropertyId(this.objectId);
        }
        return this.id;
    }
}
