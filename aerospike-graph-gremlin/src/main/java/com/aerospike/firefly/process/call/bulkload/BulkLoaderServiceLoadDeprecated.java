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

package com.aerospike.firefly.process.call.bulkload;

import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Map;

public class BulkLoaderServiceLoadDeprecated<I, R> extends BulkLoaderServiceLoad<I, R> {
    public BulkLoaderServiceLoadDeprecated(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    public String getName() {
        return "bulk-load";
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.warn("Invoked deprecated API '" + getName() + "' for future use please see '" + super.getName() + "'.");
        super.auditLog(params);
    }

    @Override
    public boolean needRouting() {
        return false;
    }
}
