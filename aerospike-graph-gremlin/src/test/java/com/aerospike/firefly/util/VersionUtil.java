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

package com.aerospike.firefly.util;

import java.util.ArrayList;
import java.util.List;

public class VersionUtil {
    final String version;

    public VersionUtil(final String version) {
        if (version == null) {
            throw new IllegalArgumentException("Version cannot be null.");
        }
        final String versionLower = version.toLowerCase();
        this.version = versionLower.endsWith("-snapshot") ?
                versionLower.substring(0, versionLower.indexOf("-snapshot")) : versionLower;
    }

    public String getVersion() {
        return version;
    }

    public int getMajor() {
        return Integer.parseInt(version.substring(0, version.indexOf(".")));
    }

    public int getMinor() {
        return Integer.parseInt(version.substring(version.indexOf(".") + 1, version.lastIndexOf(".")));
    }

    public int getPatch() {
        return Integer.parseInt(version.substring(version.lastIndexOf(".") + 1));
    }

    public boolean hasPreviousPatch() {
        return getPatch() != 0;
    }

    public boolean hasPreviousMinor() {
        return getMinor() != 0;
    }

    public String getPreviousPatch() {
        if (!hasPreviousPatch()) {
            throw new IllegalStateException("No previous patch version available.");
        }
        return String.format("%d.%d.%d", getMajor(), getMinor(), getPatch() - 1);
    }

    public String getNextPatch() {
        return String.format("%d.%d.%d", getMajor(), getMinor(), getPatch() + 1);
    }

    public List<String> getAllPatches(final int minor) {
        final List<String> patches = new ArrayList<>();
        for (int i = 0; i < getPatch(); i++) {
            patches.add(String.format("%d.%d.%d", getMajor(), minor, i));
        }
        return patches;
    }

    public List<String> getAllMinors() {
        final List<String> minors = new ArrayList<>();
        for (int i = 0; i < getMinor(); i++) {
            minors.add(String.format("%d.%d.%d", getMajor(), i, 0));
        }
        return minors;
    }

    public String getPreviousMinor() {
        if (!hasPreviousMinor()) {
            throw new IllegalStateException("No previous minor version available.");
        }
        return String.format("%d.%d.%d", getMajor(), getMinor() - 1, 0);
    }
}
