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

import java.util.Optional;

public class ReadContext {
    private final String setName;
    private final Optional<String> binName;
    private final Optional<String> keyName;

    public ReadContext(final String setName, final Optional<String> binName, final Optional<String> keyName) {
        this.setName = setName;
        this.binName = binName;
        this.keyName = keyName;
    }

    public static ReadContext create(final String set) {
        return new ReadContext(set, Optional.empty(), Optional.empty());
    }

    public static ReadContext create(final String set, final String bin) {
        return new ReadContext(set, Optional.of(bin), Optional.empty());
    }

    public static ReadContext create(final String set, final String bin, final String key) {
        return new ReadContext(set, Optional.ofNullable(bin), Optional.ofNullable(key));
    }


    public String getSetName() {
        return setName;
    }

    public Optional<String> getBinName() {
        return binName;
    }

    public Optional<String> getKeyName() {
        return keyName;
    }

}
