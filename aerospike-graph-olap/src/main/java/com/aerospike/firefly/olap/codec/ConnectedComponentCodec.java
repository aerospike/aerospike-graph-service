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

package com.aerospike.firefly.olap.codec;

import com.aerospike.firefly.olap.structure.MutableDetachedVertexProperty;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.firefly.olap.codec.RowCodec.HALTED_COL;
import static com.aerospike.firefly.olap.codec.RowCodecHelper.getId;
import static com.aerospike.firefly.olap.codec.RowCodecHelper.getIdType;

public class ConnectedComponentCodec implements Codec {
    public static final String COMPONENT_COL = "~component";

    private final String property;
    private final TraverserGenerator traverserGenerator;

    public ConnectedComponentCodec(final Traversal traversal, final String property) {
        this.traverserGenerator = traversal.asAdmin().getTraverserGenerator();
        this.property = property;
    }

    @Override
    public Row encode(final Traverser traverser) {
        final Vertex v = (Vertex) traverser.get();

        final List<Object> objects = new ArrayList<>();
        objects.add(v.id().toString()); // String id.
        objects.add(getIdType(v.id()).ordinal());

        final VertexProperty component = v.property(property);
        objects.add(component.isPresent() ? component.value() : v.id().toString()); // starting component

        // not halted for now
        objects.add(false);

        return RowFactory.create(objects.toArray(new Object[0]));
    }

    @Override
    public Traverser decode(final Row row) {
        final Object id = getId(row.getString(0), row.getInt(1));

        final String component = row.get(2) == null ? id.toString() : row.getString(2);

        final MutableDetachedVertexProperty componentProperty = new MutableDetachedVertexProperty(null, property, component, null);

        final DetachedVertex vertex = new DetachedVertex(id, "", List.of(componentProperty));
        return traverserGenerator.generate(vertex, EmptyStep.instance(), 1);
    }

    @Override
    public StructType getSchema() {
        return new StructType()
                .add(ELEMENT_ID_COL, DataTypes.StringType, true)
                .add(ELEMENT_ID_TYPEHINT_COL, DataTypes.IntegerType, true)
                .add(COMPONENT_COL, DataTypes.StringType, true)
                .add(HALTED_COL, DataTypes.BooleanType, false);
    }

    @Override
    public TraverserGenerator getTraverserGenerator() {
        return this.traverserGenerator;
    }
}
