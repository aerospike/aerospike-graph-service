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
     * Given a particular dataModel class, take a look at the on-disk data and see if we need to execute upgrade tasks.
     * Note, we do not have upgrade tasks and likely won't do this through the graph in this fashion, so we never
     * actually return true, we either throw an exception or return false.
     *
     * @param dataModel configured class implementing the data model to use
     * @param db        AerospikeConnection instance
     * @return does it need to run upgrade?
     * @throws Exception if the data model is not compatible with the current version.
     */
    public static boolean checkNeedsUpgrade(final Class<? extends FireflyGraph> dataModel, final AerospikeConnection db)
            throws Exception {
        final AerospikeConnection.GraphMetadata driveGraph = db.getDataModelMetadata();
        final ComparableVersion driveVersion = driveGraph.getDataModelVersion();
        final String driveDataModel = driveGraph.getDataModelName();
        final String classDataModel = (String) dataModel.getMethod(GETDATAMODELNAME).invoke(null);
        final ComparableVersion classVersion = (ComparableVersion) dataModel.getMethod(DATAMODELVERSION).invoke(null);


        if (driveVersion == null && driveDataModel == null) {
            // If both are null then this is a fresh system.
            db.setGraphMetadata(classDataModel, classVersion.toString());

            // Return false, no upgrade required.
            return false;
        } else if (driveVersion == null || driveDataModel == null) {
            // This should never happen, they should either both be null or neither.
            // Throw just to be safe.
            throw new RuntimeException(driveVersion == null ? "currentVer is null." : "currentModel is null.");
        } else {
            final int driveVersionMajor = Integer.parseInt(driveGraph.getDataModelVersion().toString().split("\\.")[0]);
            final int classVersionMajor = Integer.parseInt(classVersion.toString().split("\\.")[0]);

            // If the data models match then we need to check the versions.
            if (driveVersionMajor != classVersionMajor) {
                // If the on disk data model is > software data model we can't upgrade. This is illegal, send them to jail.
                throw new RuntimeException(String.format("The on disk data model major version '%s' does not match the " +
                        "data model '%s' being used.", driveVersion, classVersion));

            }

            // Major versions match, upgrade not required.
            return false;
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
