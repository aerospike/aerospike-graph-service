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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class RepoPaths {
    private static final String PLACEHOLDER = "${repo.root}";
    private static final List<String> LEGACY_REPO_ROOTS = List.of(
            "/actions-runner/_work/firefly/firefly",
            "/home/runner/work/firefly/firefly");

    private RepoPaths() {
    }

    public static String repoRoot() {
        for (final String envVar : List.of("GITHUB_WORKSPACE", "FIREFLY_REPO_ROOT")) {
            final String value = System.getenv(envVar);
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        for (final String legacyRoot : LEGACY_REPO_ROOTS) {
            if (Files.isDirectory(Path.of(legacyRoot))) {
                return legacyRoot;
            }
        }
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null) {
            if (Files.exists(current.resolve("pom.xml")) && Files.exists(current.resolve("conf"))) {
                return current.toString();
            }
            current = current.getParent();
        }
        throw new IllegalStateException("Cannot locate repository root");
    }

    public static String expand(final String value) {
        if (value == null) {
            return null;
        }
        String expanded = value.replace(PLACEHOLDER, repoRoot());
        for (final String legacyRoot : LEGACY_REPO_ROOTS) {
            expanded = expanded.replace(legacyRoot, repoRoot());
        }
        return expanded;
    }
}
