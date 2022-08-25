package com.aerospike.firefly.io;

import com.aerospike.firefly.structure.FireflyGraph;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class DataModelInfo {
    private final Class<? extends FireflyGraph> impl;
    private final AerospikeConnection db;

    public DataModelInfo(Class<? extends FireflyGraph>  impl, AerospikeConnection db){
        this.impl = impl;
        this.db = db;
    }
    public Class<? extends FireflyGraph> modelName(){
        return impl;
    }
    public int onDiskVersion(){
        return db.getModelVersion();
    }

}
