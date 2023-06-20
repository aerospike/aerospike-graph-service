package com.aerospike.firefly.io;

import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface UpgradeTask {
    //This task supports upgrading from version x to version y;
    public Map.Entry<String, String> upgradePath();

    //Data model impl class this task supports upgrading;
    public Class<? extends FireflyGraph> dataModel();

    //Execute the upgrade task
    public void performUpgrade(AerospikeConnection db) throws Exception;
}
