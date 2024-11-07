package com.aerospike.firefly.io.aerospike;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.exceptions.DataModelVersionMismatchException;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class DataModelVersioning {
    static private final Logger LOG = LoggerFactory.getLogger(DataModelVersioning.class);

    /**
     * Check to see if the current Aerospike Graph version is compatible with the version in Aerospike Database.
     *
     * @param db AerospikeConnection instance
     */
    public static void checkVersionCompatibility(final AerospikeConnection db) {
        final AerospikeConnection.GraphMetadata driveGraph = db.getDataModelMetadata();
        final ComparableVersion driveVersion = driveGraph.getDataModelVersion();
        final String driveDataModel = driveGraph.getDataModelName();
        final String classDataModel = FireflyGraph.getDataModelName();
        final ComparableVersion classVersion = FireflyGraph.dataModelVersion();

        if (driveVersion == null && driveDataModel == null) {
            // If both are null then this is a fresh system.
            db.setGraphMetadata(classDataModel, classVersion.toString());
        } else if (driveVersion == null || driveDataModel == null) {
            // This should never happen, they should either both be null or neither.
            // Throw just to be safe.
            throw new IllegalStateException(driveVersion == null ? "currentVer is null." : "currentModel is null.");
        } else {
            final String[] splitDriveVersion = driveVersion.toString().split("\\.");
            final String[] splitClassVersion = classVersion.toString().split("\\.");
            final int driveVersionMajor = Integer.parseInt(splitDriveVersion[0]);
            final int classVersionMajor = Integer.parseInt(splitClassVersion[0]);
            final int driveMinorVersion = Integer.parseInt(splitDriveVersion[1]);
            final int classMinorVersion = Integer.parseInt(splitClassVersion[1]);

            // Major version must match and AGS minor version must not be lower than disk version
            if (driveVersionMajor != classVersionMajor || classMinorVersion < driveMinorVersion) {
                // Get sent to jail kid.
                throw new DataModelVersionMismatchException(driveVersion, classVersion);
            }

            // Disable supernode adjacency pushdown filters if existing data model is on 2.0.0
            if (driveVersionMajor == 2) {
                if (driveMinorVersion < 1) {
                    LOG.warn("Aerospike Graph Service detected existing data model on version 2.0.x during startup. " +
                            "Supernode edge filtering optimizations will run in compatibility mode.");
                    db.isSupernodePushdownEnabled = false;
                }
                if (driveMinorVersion < 3) {
                    LOG.warn("Aerospike Graph Service detected existing data model on version 2.1 or lower during startup. " +
                            "MergeEdge step optimizations will run in compatibility mode.");
                    db.isMergeEdgeDataModelEnabled = false;
                }
            }
        }
    }
}
