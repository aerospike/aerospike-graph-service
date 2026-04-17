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

package com.aerospike.firefly.olap.process;

import com.aerospike.firefly.olap.codec.Codec;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

import java.util.Set;

public interface FireflyProgram extends VertexProgram<TraverserSet<Object>> {
    void execute(final BatchJob job, final Memory memory);

    Codec getCodec();

    PureTraversal<?, ?> getTraversal();

    default void postProcessResults(final TraverserSet traversers, final Memory memory) {}

    default Pair<Boolean, Dataset<Row>> postProcessResults(final Dataset<Row> dataset, final Memory memory) { return Pair.with(false, null); }

    default void execute(final Vertex vertex, final Messenger<TraverserSet<Object>> messenger, final Memory memory) {
        throw new UnsupportedOperationException("Not supported.");
    }

    default Set<MessageScope> getMessageScopes(final Memory memory) {
        return Set.of();
    }

    default boolean validPostProcessSteps() {
        return true;
    }

    void setJobId(final String jobId);
    void initDB();
    void cleanUpDB();
}
