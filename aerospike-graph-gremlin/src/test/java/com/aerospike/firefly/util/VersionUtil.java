package com.aerospike.firefly.util;

import java.util.ArrayList;
import java.util.List;

public class VersionUtil {
    final String version;

    public VersionUtil(final String version) {
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
