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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

import static com.aerospike.firefly.util.Tokens.EDGE_UNIQUE_ID_COUNTER;
import static com.aerospike.firefly.util.Tokens.EDGE_PACKING_ID_COUNTER;
import static com.aerospike.firefly.util.Tokens.VERTEX_ID_COUNTER;
import static com.aerospike.firefly.util.Tokens.VERTEX_PROPERTY_ID_COUNTER;

public abstract class BufferedNumericIdManager implements IdManager<Long> {
    protected static final Logger LOG = LoggerFactory.getLogger(BufferedNumericIdManager.class);
    private final long bufferSize;
    private AtomicLong bufferedId = null;
    private AtomicLong bufferTrigger = null;
    protected final String counterName;
    protected final String readableIdName;

    // Bulk loader mode specifics
    private static final Object VP_LOCK = new Object();
    private static final Object VID_LOCK = new Object();
    private static final Object EID_PACK_LOCK = new Object();
    private static final Object EID_UNIQUE_LOCK = new Object();

    private static final AtomicLong VP_ID = new AtomicLong(Long.MAX_VALUE);
    private static final AtomicLong VP_ID_TRIGGER = new AtomicLong(Long.MAX_VALUE);
    private static final AtomicLong VERTEX_ID = new AtomicLong(Long.MAX_VALUE);
    private static final AtomicLong VERTEX_ID_TRIGGER = new AtomicLong(Long.MAX_VALUE);
    private static final AtomicLong EP_ID = new AtomicLong(Long.MAX_VALUE);
    private static final AtomicLong EP_ID_TRIGGER = new AtomicLong(Long.MAX_VALUE);
    private static final AtomicLong EU_ID = new AtomicLong(Long.MAX_VALUE);
    private static final AtomicLong EU_ID_TRIGGER = new AtomicLong(Long.MAX_VALUE);

    protected BufferedNumericIdManager(final String counterName, final long bufferSize) {
        this.counterName = counterName;
        this.bufferSize = bufferSize;
        switch (counterName) {
            case VERTEX_PROPERTY_ID_COUNTER:
                readableIdName = "VP IDs";
                break;
            case VERTEX_ID_COUNTER:
                readableIdName = "Vertex IDs";
                break;
            case EDGE_UNIQUE_ID_COUNTER:
                readableIdName = "Edge unique IDs";
                break;
            case EDGE_PACKING_ID_COUNTER:
                readableIdName = "Edge packing IDs";
                break;
            default:
                throw new IllegalArgumentException("Unknown counter name '" + counterName + "'.");
        }
    }

    @Override
    public synchronized Long getNextId(final FireflyGraph graph) {
        if (graph.bulkLoaderFlag) {
            return getNextBulkLoaderId(graph);
        }

        if (this.bufferedId == null) {
            this.bufferedId = new AtomicLong();
            this.bufferTrigger = new AtomicLong();
            bufferIds(graph, this.bufferSize, this.bufferedId, this.bufferTrigger);
        }

        return getNextIdFromBuffers(graph, this.bufferSize, this.bufferedId, this.bufferTrigger);
    }

    abstract protected Long getNextIdFromBuffers(final FireflyGraph graph, final long bufferSize,
                                                 final AtomicLong idTracker, final AtomicLong idTrigger);


    abstract protected void bufferIds(final FireflyGraph graph, final long bufferSize, final AtomicLong idTracker,
                           final AtomicLong idTrigger);

    @Override
    public void recycleId(final FireflyId id, final FireflyGraph graph, final boolean wasIdCommitted) {
        throw new RuntimeException("Recycling IDs is not supported by BufferedNumericIdManager.");
    }

    private long getNextBulkLoaderId(final FireflyGraph graph) {
        switch (this.counterName) {
            case VERTEX_PROPERTY_ID_COUNTER:
                synchronized (VP_LOCK) {
                    return getBulkLoaderId(graph, VP_ID, VP_ID_TRIGGER);
                }
            case VERTEX_ID_COUNTER:
                synchronized (VID_LOCK) {
                    return getBulkLoaderId(graph, VERTEX_ID, VERTEX_ID_TRIGGER);
                }
            case EDGE_UNIQUE_ID_COUNTER:
                synchronized (EID_PACK_LOCK) {
                    return getBulkLoaderId(graph, EU_ID, EU_ID_TRIGGER);
                }
            case EDGE_PACKING_ID_COUNTER:
                synchronized (EID_UNIQUE_LOCK) {
                    return getBulkLoaderId(graph, EP_ID, EP_ID_TRIGGER);
                }
            default:
                throw new IllegalArgumentException("Unknown bulk load counter name '" + counterName + "'.");
        }
    }

    private long getBulkLoaderId(final FireflyGraph graph, final AtomicLong idTracker, final AtomicLong idTrigger) {
        if (idTracker.get() == Long.MAX_VALUE) {
            bufferIds(graph, graph.bulkLoadIdBufferSize, idTracker, idTrigger);
        }
        return getNextIdFromBuffers(graph, graph.bulkLoadIdBufferSize, idTracker, idTrigger);
    }
}
