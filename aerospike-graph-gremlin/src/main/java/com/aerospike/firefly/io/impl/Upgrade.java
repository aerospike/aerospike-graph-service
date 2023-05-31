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

    private static final List<Class<? extends UpgradeTask>> availableUpgrades = new ArrayList<>();

    /**
     * Configure an upgrade task that will mutate the Aerospike data before the Graph is loaded
     * This should match a particular "from" and "to" version, and should update the on-disk version accordingly
     *
     * @param task class that implements UpgradeTask
     */
    public static void registerUpgradeTask(Class<? extends UpgradeTask> task) {
        availableUpgrades.add(task);
    }

    /**
     * Write the metadata into the Aerospike database representing data model and version
     *
     * @param dataModel class implementing the current data model
     * @param db        AerospikeConnection instance
     * @throws Exception if the data model is not compatible with the current version.
     */
    public static void initMetadata(Class<? extends FireflyGraph> dataModel, AerospikeConnection db) throws Exception {
        ComparableVersion classModelVer = (ComparableVersion) dataModel.getMethod(DATAMODELVERSION).invoke(null);
        String dataModelName = (String) dataModel.getMethod(GETDATAMODELNAME).invoke(null);

        db.setModelVersion(classModelVer.toString());
        db.setModelName(dataModelName);
    }

    /**
     * Given a particular dataModel class, take a look at the on-disk data and see if we need to execute upgrade tasks
     *
     * @param dataModel configured class implementing the data model to use
     * @param db        AerospikeConnection instance
     * @return does it need to run upgrade?
     * @throws Exception if the data model is not compatible with the current version.
     */
    public static boolean checkNeedsUpgrade(Class<? extends FireflyGraph> dataModel, AerospikeConnection db) throws Exception {
        ComparableVersion currentVer = db.getDataModelVerion();
        String currentModel = db.getDataModelName();
        if (currentVer == null && currentModel == null) {
            initMetadata(dataModel, db);
            currentVer = db.getDataModelVerion();
            currentModel = db.getDataModelName();
        } else if (currentVer == null || currentModel == null) {
            throw new RuntimeException(currentVer == null ? "currentVer is null." : "currentModel is null.");
        }
        if (!dataModel.getMethod(GETDATAMODELNAME).invoke(null).equals(currentModel))
            throw new RuntimeException(String.format("On disk data model '%s' does not match provided data model '%s'.", currentModel, dataModel.getCanonicalName()));
        ComparableVersion classModelVer = (ComparableVersion) dataModel.getMethod(DATAMODELVERSION).invoke(null);
        if (currentVer == null)
            db.setModelVersion(classModelVer.toString());
        if (classModelVer.compareTo(db.getDataModelVerion()) < 0)
            throw new RuntimeException(String.format("On disk data model '%s' is higher then software data model '%s'.", db.getDataModelVerion(), classModelVer));
        return db.getDataModelVerion().compareTo(classModelVer) < 0;
    }

    /**
     * Invoke the list of upgrade tasks that match the on-disk version as "from" and running code version as "to"
     *
     * @param dataModel class implementing current data model
     * @param db        AerospikeConnection instance
     * @throws Exception if the data model is not compatible with the current version.
     */
    public static void performUpgrade(Class<? extends FireflyGraph> dataModel, AerospikeConnection db) throws Exception {
        ComparableVersion currentVer = db.getDataModelVerion();
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
