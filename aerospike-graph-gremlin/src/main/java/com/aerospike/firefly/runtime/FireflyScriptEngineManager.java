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

package com.aerospike.firefly.runtime;

import org.apache.tinkerpop.gremlin.jsr223.CachedGremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;

public class FireflyScriptEngineManager extends CachedGremlinScriptEngineManager {
    public FireflyScriptEngineManager() {
        super();
    }

    @Override
    public GremlinScriptEngine getEngineByName(final String shortName) {
        // everything is gremlin-lang
        return super.getEngineByName("gremlin-lang");
    }

    @Override
    public GremlinScriptEngine getEngineByExtension(final String extension) {
        return super.getEngineByName("gremlin-lang");
    }

    @Override
    public GremlinScriptEngine getEngineByMimeType(final String mimeType) {
        return super.getEngineByName("gremlin-lang");
    }
}
