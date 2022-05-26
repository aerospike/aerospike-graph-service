package com.aerospike.firefly.io;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.async.Throttles;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.WritePolicy;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */

class ScanRecordSequenceListener implements RecordSequenceListener {
    private final AerospikeClient client;
    private EventLoops eventLoops;
    private Throttles throttles;
    private Monitor scanMonitor;
    private AtomicInteger writeCount = new AtomicInteger();
    private int scanCount = 0;
    private final int progressFreq;

    public ScanRecordSequenceListener(EventLoops eventLoops,
                                      Throttles throttles,
                                      Monitor scanMonitor,
                                      AerospikeClient client,
                                      int progressFreq) {
        this.eventLoops = eventLoops;
        this.throttles = throttles;
        this.scanMonitor = scanMonitor;
        this.progressFreq = progressFreq;
        this.client = client;
    }

    public void onRecord(Key key, Record record) throws AerospikeException {
        ++scanCount;
        if (progressFreq > 0 && scanCount % progressFreq == 0) {
            System.out.format("Scan returned %s records.\n", scanCount);
        }
        // submit async update operation with throttle
        EventLoop eventLoop = eventLoops.next();
        int eventLoopIndex = eventLoop.getIndex();
        if (throttles.waitForSlot(eventLoopIndex, 1)) {      // throttle by waiting for an available slot
            try {
                WritePolicy policy = new WritePolicy();
                Bin bin2 = new Bin(new String("bin2"), 1);

                client.add(eventLoop, new WriteListener() {  // inline write listener

                            public void onSuccess(final Key key) {
                                // Write succeeded.
                                throttles.addSlot(eventLoopIndex, 1);
                                int currentCount = writeCount.incrementAndGet();
                                if (progressFreq > 0 && currentCount % progressFreq == 0) {
                                    System.out.format("Processed %s records.\n", currentCount);
                                }
                            }

                            public void onFailure(AerospikeException e) {
                                System.out.format("Put failed: namespace=%s set=%s key=%s exception=%s\n",
                                        key.namespace, key.setName, key.userKey, e.getMessage());
                                throttles.addSlot(eventLoopIndex, 1);
                                int currentCount = writeCount.incrementAndGet();
                                if (progressFreq > 0 && currentCount % progressFreq == 0) {
                                    System.out.format("Processed %s records.\n", currentCount);
                                }
                            }
                        },
                        policy, key, bin2);
            } catch (Exception e) {
                System.out.format("Error: exception in write listener - %s", e.getMessage());
            }
        }
    }

    public void onSuccess() {
        if (scanCount != writeCount.get()) {   // give the last write some time to finish
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                System.out.format("Error: exception - %s", e);
            }
        }
        scanMonitor.notifyComplete();
    }

    public void onFailure(AerospikeException e) {
        System.out.format("Error: scan failed with exception - %s", e);
        scanMonitor.notifyComplete();
    }
}
