package com.aerospike.firefly.io.impl;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.UpgradeTask;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.maven.artifact.versioning.ComparableVersion;

import java.util.ArrayList;
import java.util.List;

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
     * Given a particular dataModel class, take a look at the on-disk data and see if we need to execute upgrade tasks
     *
     * @param dataModel configured class implementing the data model to use
     * @param db        AerospikeConnection instance
     * @return does it need to run upgrade?
     * @throws Exception if the data model is not compatible with the current version.
     */
    public static boolean checkNeedsUpgrade(Class<? extends FireflyGraph> dataModel, AerospikeConnection db) throws Exception {
        final ComparableVersion driveVersion = db.getDataModelMetadata().getDataModelVersion();
        final String driveDataModel = db.getDataModelMetadata().getDataModelName();
        final String classDataModel = (String) dataModel.getMethod(GETDATAMODELNAME).invoke(null);
        final ComparableVersion classVersion = (ComparableVersion) dataModel.getMethod(DATAMODELVERSION).invoke(null);

        if (driveVersion == null && driveDataModel == null) {
            // If both are null then this is a fresh system.
            db.setGraphMetadata(classDataModel, classVersion.toString());
            return false;
        } else if (driveVersion == null || driveDataModel == null) {
            // This should never happen.
            throw new RuntimeException(driveVersion == null ? "currentVer is null." : "currentModel is null.");
        } else {
            // If both are not null then we need to check if the data model and versions are the same.
            if (!classDataModel.equals(driveDataModel)) {
                // Data model mismatch is illegal, send them to jail.
                throw new RuntimeException(String.format("On disk data model '%s' does not match provided data model " +
                        "'%s'.", driveDataModel, dataModel.getCanonicalName()));
            }
            // If the data models match then we need to check the versions.
            if (classVersion.compareTo(driveVersion) < 0) {
                // If the on disk data model is > software data model we can't upgrade. This is illegal, send them to jail.
                throw new RuntimeException(String.format("On disk data model '%s' is higher then software data model " +
                        "'%s'.", driveVersion, classVersion));

            }

            // Returns true if drive version < class version, meaning we need to upgrade.
            return driveVersion.compareTo(classVersion) < 0;
        }
    }

    /**
     * Invoke the list of upgrade tasks that match the on-disk version as "from" and running code version as "to"
     *
     * @param dataModel class implementing current data model
     * @param db        AerospikeConnection instance
     * @throws Exception if the data model is not compatible with the current version.
     */
    public static void performUpgrade(Class<? extends FireflyGraph> dataModel, AerospikeConnection db) throws Exception {
        throw new RuntimeException("Error, current drive version (" + db.getDataModelMetadata().getDataModelVersion() +
                ") of Aerospike Graph is incompatible with the current software version (" +
                dataModel.getMethod(DATAMODELVERSION).invoke(null) + ").");
    }
}
