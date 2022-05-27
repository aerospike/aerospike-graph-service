package com.aerospike.firefly.io;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.async.Throttles;
import com.aerospike.client.listener.RecordSequenceListener;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */

class ScanRecordSequenceListener implements RecordSequenceListener {
    private final AerospikeClient client;
    private final EventLoops eventLoops;
    private final Throttles throttles;
    private final Monitor scanMonitor;
    private final AtomicInteger writeCount = new AtomicInteger();
    private final ConcurrentLinkedQueue<Map.Entry<Key, Record>> results = new ConcurrentLinkedQueue<>();
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
        results.add(new AbstractMap.SimpleEntry<>(key, record));
    }

    public void onSuccess() {
        scanMonitor.notifyComplete();
    }

    public void onFailure(AerospikeException e) {
        System.out.format("Error: scan failed with exception - %s", e);
        scanMonitor.notifyComplete();
    }

    public Iterator<Map.Entry<Key, Record>> iterator() {
        return results.iterator();
    }
}
