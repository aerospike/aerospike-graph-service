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

package com.aerospike.firefly.olap.service;

import com.aerospike.firefly.olap.process.packing.DistributedAerospikeConnection;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JobCancellationService<I, R> implements Service.ServiceFactory<I, R>, Service<I, R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobCancellationService.class);

    @Override
    public Service.Type getType() {
        return Service.Type.Start;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Service.super.getRequirements();
    }

    // start
    @Override
    public CloseableIterator execute(final Service.ServiceCallContext ctx, final Map params) {
        final DistributedAerospikeConnection db = (DistributedAerospikeConnection) params.get("db");
        // possible to have other running or lost jobs
        db.cancelAllJobs();

        LOGGER.warn("All jobs cancelled.");

        final List<String> result = List.of("All jobs cancelled.");
        return CloseableIterator.of(result.iterator());
    }

    @Override
    public String getName() {
        return "aerospike.graph.analytics.job.cancel";
    }

    @Override
    public Set<Service.Type> getSupportedTypes() {
        return Collections.singleton(Service.Type.Start);
    }

    @Override
    public Service<I, R> createService(final boolean isStart, final Map params) {
        return this;
    }

    @Override
    public void close() {
        Service.ServiceFactory.super.close();
    }
}
