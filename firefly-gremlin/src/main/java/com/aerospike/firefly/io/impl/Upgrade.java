package com.aerospike.firefly.io.impl;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.UpgradeTask;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.maven.artifact.versioning.ComparableVersion;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.structure.FireflyGraph.DATAMODELVERSION;
import static com.aerospike.firefly.structure.FireflyGraph.GETDATAMODELNAME;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Upgrade {
    public static class DataModelMetadata {
        public final int version;
        public final String name;

        public DataModelMetadata(int version, String name) {
            this.version = version;
            this.name = name;
        }
    }

    private static List<Class<? extends UpgradeTask>> availableUpgrades = new ArrayList<>();

    public static void registerUpgradeTask(Class<? extends UpgradeTask> task) {
        availableUpgrades.add(task);
    }


    public static void initMetadata(Class<? extends FireflyGraph> dataModel, AerospikeConnection db) throws Exception {
        ComparableVersion classModelVer = (ComparableVersion) dataModel.getMethod(DATAMODELVERSION).invoke(null);
        String dataModelName = (String) dataModel.getMethod(GETDATAMODELNAME).invoke(null);

        db.setModelVersion(classModelVer.toString());
        db.setModelName(dataModelName);
    }

    public static boolean checkNeedsUpgrade(Class<? extends FireflyGraph> dataModel, AerospikeConnection db) throws Exception {
        ComparableVersion currentVer = db.getModelVersion();
        String currentModel = db.getDataModelName();
        if (currentVer == null && currentModel == null) {
            initMetadata(dataModel, db);
            currentVer = db.getModelVersion();
            currentModel = db.getDataModelName();
        } else if (currentVer == null || currentModel == null){
            throw new RuntimeException("currentVer or currentModel null, but not both");
        }
        if (!dataModel.getMethod(GETDATAMODELNAME).invoke(null).equals(currentModel))
            throw new RuntimeException(String.format("On disk data model %s does not match provided data model %s", currentModel, dataModel.getCanonicalName()));
        ComparableVersion classModelVer = (ComparableVersion) dataModel.getMethod(DATAMODELVERSION).invoke(null);
        if (currentVer == null)
            db.setModelVersion(classModelVer.toString());
        if (classModelVer.compareTo(db.getModelVersion()) < 0)
            throw new RuntimeException(String.format("On disk data model %d is higher then software data model %d", db.getModelVersion(), classModelVer));
        return db.getModelVersion().compareTo(classModelVer) < 0;
    }

    public static void performUpgrade(Class<? extends FireflyGraph> dataModel, AerospikeConnection db) throws Exception {
        ComparableVersion currentVer = db.getModelVersion();
        ComparableVersion classVer = (ComparableVersion) dataModel.getMethod(DATAMODELVERSION).invoke(null);
        if (checkNeedsUpgrade(dataModel, db)) {
            for (Class<? extends UpgradeTask> task : availableUpgrades) {
                UpgradeTask upgradeTask = task.getConstructor().newInstance();
                Map.Entry<String, String> upgradePath = new AbstractMap.SimpleEntry<>(currentVer.toString(), classVer.toString());
                if (upgradeTask.dataModel().isAssignableFrom(dataModel) && upgradeTask.upgradePath().equals(upgradePath))
                    upgradeTask.performUpgrade(db);
            }
        }
    }
}
