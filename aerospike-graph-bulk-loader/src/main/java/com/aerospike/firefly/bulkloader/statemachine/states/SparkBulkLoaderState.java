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

import java.util.Map;

public abstract class SparkBulkLoaderState {
    protected SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine;

    public SparkBulkLoaderState(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        this.sparkBulkLoaderStateMachine = sparkBulkLoaderStateMachine;
    }
    public abstract void executeState();
    public abstract SparkBulkLoaderState transitionState();
    protected abstract BulkLoadStateStatusMap getStateMap();
    public final Map<String, Object> getStateStatus() {
        return this.getStateMap();
    }
}
