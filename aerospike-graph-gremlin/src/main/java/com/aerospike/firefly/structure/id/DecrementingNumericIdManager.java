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

import java.util.concurrent.atomic.AtomicLong;

public class DecrementingNumericIdManager extends BufferedNumericIdManager {

    protected DecrementingNumericIdManager(final String counterName, final long bufferSize) {
        super(counterName, bufferSize);
    }

    @Override
    protected Long getNextIdFromBuffers(final FireflyGraph graph, final long bufferSize, final AtomicLong idTracker,
                                        final AtomicLong idTrigger) {
        final long id = idTracker.getAndDecrement();
        if (id <= idTrigger.get()) {
            // The ID we got is the last reserved ID so we need to buffer more
            bufferIds(graph, bufferSize, idTracker, idTrigger);
        }
        return id;
    }

    @Override
    protected void bufferIds(final FireflyGraph graph, final long bufferSize, final AtomicLong idTracker,
                             final AtomicLong idTrigger) {
        LOG.info("Allocating batch of {} {}.", bufferSize, this.readableIdName);
        // This is the new last reserved ID
        idTrigger.set(graph.getBaseGraph().addIdCounter(this.counterName, -bufferSize));
        idTracker.set(idTrigger.get() + bufferSize - 1);
    }
}
