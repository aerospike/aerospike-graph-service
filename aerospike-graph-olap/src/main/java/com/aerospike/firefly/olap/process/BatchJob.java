package com.aerospike.firefly.olap.process;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;

public class BatchJob {
    private TraverserSet<Object> starts;
    private final TraverserSet<Traverser.Admin> results;

    public BatchJob(final TraverserSet<Object> starts) {
        this.starts = starts;
        this.results = new TraverserSet<>();
    }

    public TraverserSet<Object> getStarts() {
        return starts;
    }

    public TraverserSet<Traverser.Admin> getResults() {
        return results;
    }

    public void addResult(final Traverser.Admin traverser) {
        this.results.add(traverser);
    }

    public void setStarts(final TraverserSet<Object> starts) {
        this.starts = starts;
    }

    public void clear() {
        this.starts.clear();
        this.results.clear();
    }

}
