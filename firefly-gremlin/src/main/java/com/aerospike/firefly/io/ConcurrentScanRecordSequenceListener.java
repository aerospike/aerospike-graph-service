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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */

class ConcurrentScanRecordSequenceListener implements RecordSequenceListener {
    private final AerospikeClient client;
    private final EventLoops eventLoops;
    private final Throttles throttles;
    private final Monitor scanMonitor;
    private final LinkedBlockingQueue<Map.Entry<Key, Record>> results = new LinkedBlockingQueue<>();
    private final int progressFreq;
    private Semaphore semaphore;
    private AtomicBoolean complete = new AtomicBoolean(false);

    public ConcurrentScanRecordSequenceListener(EventLoops eventLoops,
                                                Throttles throttles,
                                                Monitor scanMonitor,
                                                AerospikeClient client,
                                                int progressFreq) {
        this.eventLoops = eventLoops;
        this.throttles = throttles;
        this.scanMonitor = scanMonitor;
        this.progressFreq = progressFreq;
        this.client = client;
        this.semaphore = new Semaphore(1);
    }

    public void onRecord(Key key, Record record) throws AerospikeException {
        triggerNext();
        results.add(new AbstractMap.SimpleEntry<>(key, record));
        semaphore.release();  // progress
    }

    private void triggerNext() {

    }

    public void onSuccess() {
        this.complete.set(true);
        scanMonitor.notifyComplete();
        semaphore.release();  // progress
    }


    public void onFailure(AerospikeException e) {
        System.out.format("Error: scan failed with exception - %s", e);
        this.complete.set(true);
        scanMonitor.notifyComplete();
        semaphore.release();  // progress
    }

    public Iterator<Map.Entry<Key, Record>> iterator() {
        return new Iterator<Map.Entry<Key, Record>>() {
            @Override
            public boolean hasNext() {
                if (results.size() > 0) return true;
                else {
                    try {
                        semaphore.acquire();
                        //next call inside wait will block until results or finished
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                return waitNext();
            }

            private boolean waitNext() {
                if(results.size()>0)
                    return true;
                try {
                    semaphore.acquire();
                    //at this point, we should either be finished the query or have a new result
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                semaphore.release();
                if(results.size()>0)
                    return true;
                return !complete.get();
            }

            @Override
            public Map.Entry<Key, Record> next() {
                try {
                    return results.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
