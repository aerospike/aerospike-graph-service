/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.io.aerospike;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.exceptions.DataModelVersionMismatchException;
import org.apache.maven.artifact.versioning.ComparableVersion;

public class DataModelVersioning {

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
        }
    }
}
