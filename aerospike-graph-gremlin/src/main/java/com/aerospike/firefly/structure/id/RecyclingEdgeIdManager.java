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

import java.nio.ByteBuffer;

abstract public class RecyclingEdgeIdManager<P extends BufferedNumericIdManager> implements IdManager<byte[]> {
    protected final long bufferSize;
    protected final P packingIdManager;
    protected final BufferedNumericIdManager uniqueIdManager;

    protected RecyclingEdgeIdManager(final P packingIdManager,
                                     final BufferedNumericIdManager uniqueIdManager,
                                     final long bufferSize) {
        this.packingIdManager = packingIdManager;
        this.uniqueIdManager = uniqueIdManager;
        this.bufferSize = bufferSize;
    }

    abstract public byte[] getNextId(final FireflyGraph graph);

    abstract protected byte[] getNewId(final FireflyGraph graph);

    abstract public void recycleId(final FireflyId id, final FireflyGraph graph, final boolean wasIdCommitted);

    static public byte[] longToBytes(final long x) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }
}
