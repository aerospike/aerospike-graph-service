package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

import static com.aerospike.firefly.util.Tokens.EDGE_UNIQUE_ID_COUNTER;
import static com.aerospike.firefly.util.Tokens.EDGE_PACKING_ID_COUNTER;
import static com.aerospike.firefly.util.Tokens.VERTEX_ID_COUNTER;
import static com.aerospike.firefly.util.Tokens.VERTEX_PROPERTY_ID_COUNTER;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class BufferedNumericIdManager implements IdManager<Long> {
    private static final Logger LOG = LoggerFactory.getLogger(BufferedNumericIdManager.class);
    private final long bufferSize;
    private AtomicLong bufferedId = null;
    private AtomicLong bufferTrigger = null;
    private final String counterName;
    private final String readableIdName;

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
            return getNextId(graph);
        }

        final long id = bufferedId.getAndDecrement();
        if (id <= this.bufferTrigger.get()) {
            // The ID we got is the last reserved ID so we need to buffer more
            bufferIds(graph, this.bufferSize, this.bufferedId, this.bufferTrigger);
        }
        return id;
    }

    private void bufferIds(final FireflyGraph graph, final long bufferSize, final AtomicLong idTracker,
                           final AtomicLong idTrigger) {
        LOG.info("Allocating batch of {} {}.", bufferSize, this.readableIdName);
        // This is the new last reserved ID
        idTrigger.set(graph.getBaseGraph().decrementIdCounter(this.counterName, bufferSize));
        // Since Firefly returns decrementing long values as generated IDs, the first buffered ID to return is
        // the largest one
        idTracker.set(idTrigger.get() + bufferSize - 1);
    }

    @Override
    public void recycleId(final FireflyId id) {
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
        final long id = idTracker.getAndDecrement();
        if (id <= idTrigger.get()) {
            // The ID we got is the last reserved ID so we need to buffer more
            bufferIds(graph, graph.bulkLoadIdBufferSize, idTracker, idTrigger);
        }
        return id;
    }
}
