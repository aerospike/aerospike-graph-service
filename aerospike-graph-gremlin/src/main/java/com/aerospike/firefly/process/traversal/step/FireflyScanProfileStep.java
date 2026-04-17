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

package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.io.aerospike.ScanHitCounter;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FireflyScanProfileStep<S> extends AbstractStep<S, S> implements Profiling {
    private MutableMetrics metrics;

    public FireflyScanProfileStep(Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    public void setMetrics(final MutableMetrics parentMetrics) {
        this.metrics = new MutableMetrics("FireflyMetrics", "FireflyScanTime");
        if (parentMetrics != null) {
            parentMetrics.addNested(this.metrics);
        }
    }

    public static long percentile(final List<Long> latencies, final double percentile) {
        final int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
        return latencies.get(index - 1);
    }

    public static double nsToMs(final long ns) {
        return ns / 1_000_000.;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        if (this.starts.hasNext()) {
            return this.starts.next();
        } else {
            throw FastNoSuchElementException.instance();
        }
    }

    @Override
    public boolean hasNext() {
        boolean res = super.hasNext();
        if (res)
            return true;
        final ScanHitCounter shc = ((FireflyGraph) this.traversal.getGraph().get()).getBaseGraph().getScanHitCounter();
        shc.stats().forEach((key, value) -> {
            this.metrics.setAnnotation(String.format(" [key: %s], scan count", key), value.get());
        });
        long sum = this.metrics.getDuration(TimeUnit.NANOSECONDS);
        for (Map.Entry<UUID, AtomicLong> entry : shc.getScanTimings().entrySet()) {
            final long nsTime = entry.getValue().get();
            this.metrics.setAnnotation(String.format(" %s : %s ", entry.getKey().toString().split("-")[0], shc.getKeyForUUID(entry.getKey())), String.format(" %.3f ms", nsTime / 1_000_000.));
            sum += nsTime;
        }
        if (sum > 0)
            this.metrics.setDuration(sum, TimeUnit.NANOSECONDS);
        ((FireflyGraph) this.traversal.getGraph().get()).getBaseGraph().resetScanHitCounter();
        return res;
    }
}
