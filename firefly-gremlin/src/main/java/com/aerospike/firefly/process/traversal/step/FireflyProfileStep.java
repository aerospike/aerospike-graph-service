package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.io.ScanHitCounter;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyProfileStep<S> extends AbstractStep<S, S> implements Profiling {
    private MutableMetrics metrics;

    public FireflyProfileStep(Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    public void setMetrics(final MutableMetrics parentMetrics) {

        this.metrics = new MutableMetrics("FireflyMetrics", "FireflyScanTime");
        if (parentMetrics != null) {
            parentMetrics.addNested(this.metrics);
        }
    }
    public static long percentile(List<Long> latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
        return latencies.get(index - 1);
    }

    public static double nsToMs(long ns) {
        return ns / 1_000_000.;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        ScanHitCounter shc = ((FireflyGraph) this.traversal.getGraph().get()).getBaseGraph().getScanHitCounter();
        shc.stats().forEach((key, value) -> {
            this.metrics.setAnnotation(String.format(" [key: %s], scan count", key), value.get());
        });
        shc.getScanTimings().entrySet().forEach((entry) -> {
            this.metrics.setAnnotation(String.format(" %s scan time ",entry.getKey().toString().split("-")[0]), String.format("%s ms",TimeUnit.NANOSECONDS.toMillis(entry.getValue().get())));
        });

        this.metrics.setDuration(0, TimeUnit.NANOSECONDS);
        if (this.starts.hasNext()) {
            return this.starts.next();
        } else {
            throw FastNoSuchElementException.instance();
        }
    }
}
