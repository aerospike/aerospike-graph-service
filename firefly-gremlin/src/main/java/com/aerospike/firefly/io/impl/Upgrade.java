package com.aerospike.firefly.io.impl;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.UpgradeTask;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Upgrade {
    private static List<Class<? extends UpgradeTask>> availableUpgrades = new ArrayList<>();

    public static void registerUpgradeTask(Class<? extends UpgradeTask> task) {
        availableUpgrades.add(task);
    }

    public static boolean checkNeedsUpgrade(Class<? extends FireflyGraph> dataModel, AerospikeConnection db) throws Exception {
        int currentVer = db.getModelVersion();
        String currentModel = db.getModelClassName();
        if (!dataModel.getCanonicalName().equals(currentModel))
            throw new RuntimeException(String.format("On disk data model %s does not match provided data model %s", currentModel, dataModel.getCanonicalName()));
        Integer classModelVer = (Integer) dataModel.getMethod("dataModelVersion").invoke(null);
        if (currentVer == 0)
            db.setModelVersion(classModelVer);
        return db.getModelVersion() < classModelVer;
    }

    public static void performUpgrade(Class<? extends FireflyGraph> dataModel, AerospikeConnection db) throws Exception {
        int currentVer = db.getModelVersion();
        int classVer = (Integer) dataModel.getMethod("dataModelVersion").invoke(null);
        if (checkNeedsUpgrade(dataModel, db)) {
            for (Class<? extends UpgradeTask> task : availableUpgrades) {
                UpgradeTask upgradeTask = task.getConstructor().newInstance();
                Map.Entry<Integer, Integer> upgradePath = new AbstractMap.SimpleEntry<>(currentVer, classVer);
                if (upgradeTask.dataModel().isAssignableFrom(dataModel) && upgradeTask.upgradePath().equals(upgradePath))
                    upgradeTask.performUpgrade(db);
            }
        }
    }
}
