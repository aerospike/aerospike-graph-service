package com.aerospike.firefly.bulkloader.util;

import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public abstract class IdHandler {
    static private final Logger LOG = LoggerFactory.getLogger(IdHandler.class);
    private final FireflyGraph graph;
    private final long bufferSize;
    private final String counterName;
    private final AtomicLong currentId;
    private long bufferTrigger;
    public final Map<String, Long> providedIdToFireflyId;

    public IdHandler(final FireflyGraph graph, final long bufferSize, final String counterName) {
        this.graph = graph;
        this.bufferSize = bufferSize;
        this.currentId = new AtomicLong();
        this.counterName = counterName;
        this.providedIdToFireflyId = new HashMap<>();
        bufferIds();
    }

    public synchronized long getId(final String providedId) {
        final long id;
        if (this.providedIdToFireflyId.containsKey(providedId)) {
            id = this.providedIdToFireflyId.get(providedId);
        } else {
            id = putId(providedId);
        }
        return id;
    }

    private long putId(final String providedId) {
        final long id = generateId();
        this.providedIdToFireflyId.put(providedId, id);
        return id;
    }

    private long generateId() {
        long id = this.currentId.getAndDecrement();
        if (this.currentId.get() < this.bufferTrigger) {
            bufferIds();
        }
        return id;
    }

    private void bufferIds() {
        // bufferTrigger represents the last reserved Id value usable before needing to buffer again
        this.bufferTrigger = this.graph.getBaseGraph().decrementIdCounter(this.counterName, this.bufferSize);
        // Set the currentId to be the first reserved Id value
        this.currentId.set(this.bufferTrigger + this.bufferSize - 1);
    }
}
