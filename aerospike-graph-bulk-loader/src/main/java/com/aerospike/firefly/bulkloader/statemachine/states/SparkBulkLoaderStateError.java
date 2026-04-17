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

package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.process.call.bulkload.BulkLoaderServiceErrors.BAD_EDGE;
import static com.aerospike.firefly.process.call.bulkload.BulkLoaderServiceErrors.BAD_ENTRY;
import static com.aerospike.firefly.process.call.bulkload.BulkLoaderServiceErrors.DUPLICATE_VID;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_EXCEPTION_MESSAGE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_EXCEPTION_STACKTRACE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_ERROR;

public class SparkBulkLoaderStateError extends SparkBulkLoaderState {
    private static final Logger LOG = LoggerFactory.getLogger(SparkBulkLoaderStateError.class);
    private final Exception error;
    private long duplicateVertexIdCount = 0;
    private long badEdgeCount = 0;
    private long badEntryCount = 0;
    private boolean errorCountParsed = false;

    public SparkBulkLoaderStateError(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine, final Exception error) {
        super(sparkBulkLoaderStateMachine);
        this.error = error;
        try {
            final AerospikeConnection db = sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph();
            this.badEntryCount = db.incrementAndGetBadEntryCount(0);
            this.duplicateVertexIdCount = db.incrementAndGetDuplicateVertexIdCount(0);
            this.badEdgeCount = db.incrementAndGetBadEdgeCount(0);
            errorCountParsed = true;
        } catch (final Exception e) {
            LOG.error("Failed to get bulk loading error counts for error state", e);
        }
    }

    @Override
    public void executeState() {
        throw new RuntimeException("Should not execute state Error");
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        throw new RuntimeException("Should not transition state Error");
    }

    @Override
    protected BulkLoadStateStatusMap getStateMap() {
        final BulkLoadStateStatusMap stateMap = new BulkLoadStateStatusMap("error", true, BULK_LOAD_STATUS_ERROR);
        stateMap.put(BULK_LOAD_EXCEPTION_MESSAGE, error.getMessage());
        stateMap.put(BULK_LOAD_EXCEPTION_STACKTRACE, ExceptionUtils.getStackTrace(error));
        if (errorCountParsed) {
            stateMap.put(DUPLICATE_VID, duplicateVertexIdCount);
            stateMap.put(BAD_ENTRY, badEntryCount);
            stateMap.put(BAD_EDGE, badEdgeCount);
        }
        return stateMap;
    }
}
