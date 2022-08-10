package com.aerospike.firefly.io;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Cache {
    Record read(Key key);
    void write(Key key, Bin... bins);
    void remove(Key key);

    void invalidate(Key key);
}
