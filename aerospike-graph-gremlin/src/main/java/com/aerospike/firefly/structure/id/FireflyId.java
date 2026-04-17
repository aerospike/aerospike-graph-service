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

package com.aerospike.firefly.structure.id;

import java.util.Arrays;

public interface FireflyId extends Comparable {

    Object getUserId();

    Object getStorageId();

    Long getStorageTypeHint();

    boolean equals(Object o);

    Object getCachedId();

    byte[] getKeyHash();

    String getKeyHashBase64();

    String getKeyHashString();

    default int compareTo(final Object o) {
        if (o instanceof FireflyId) {
            final byte[] thisStorageId = getKeyHash();
            final byte[] otherStorageId = ((FireflyId) o).getKeyHash();
            return Arrays.compare(thisStorageId, otherStorageId);
        } else {
            throw new IllegalArgumentException("Cannot compare FireflyId to " + o.getClass().getName());
        }
    }
}
