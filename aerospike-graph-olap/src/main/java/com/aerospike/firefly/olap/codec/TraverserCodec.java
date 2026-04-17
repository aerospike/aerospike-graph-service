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

import com.aerospike.firefly.olap.process.traversal.step.SparkOperation;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;

import java.util.List;
import java.util.Set;

import static com.aerospike.firefly.olap.codec.RowCodec.BULK_COL;

public class TraverserCodec implements Codec {
    private final RowCodec codec;
    private final Set<TraverserRequirement> requirements;
    private final TraversalMatrix traversalMatrix;
    private final TraverserGenerator traverserGenerator;
    private final int nativeSparkOperationColumns;

    public TraverserCodec(final Traversal traversal) {
        this.requirements = traversal.asAdmin().getTraverserRequirements();
        final List<SparkOperation> steps = TraversalHelper.getStepsOfAssignableClass(SparkOperation.class, traversal.asAdmin());
        this.nativeSparkOperationColumns = steps.stream().mapToInt(SparkOperation::sparkColumnsCount).max().orElse(0);
        this.codec = RowCodecFactory.getInstance().getCodec(this.requirements, this.nativeSparkOperationColumns);
        this.traversalMatrix = new TraversalMatrix(traversal.asAdmin());
        this.traverserGenerator = traversal.asAdmin().getTraverserGenerator();
    }

    @Override
    public Row encode(final Traverser traverser) {
        return codec.encode(traverser);
    }

    @Override
    public Traverser decode(final Row row) {
        return codec.decode(row, traverserGenerator, traversalMatrix);
    }

    @Override
    public StructType getSchema() {
        return codec.getSchema(this.nativeSparkOperationColumns);
    }

    @Override
    public TraverserGenerator getTraverserGenerator() {
        return this.traverserGenerator;
    }

    public int getBulkedOrdinal() {
        return codec.columnToOrdinal.get(BULK_COL);
    }

    public boolean isBulkingSupported() {
        return requirements.contains(TraverserRequirement.BULK);
    }
}
