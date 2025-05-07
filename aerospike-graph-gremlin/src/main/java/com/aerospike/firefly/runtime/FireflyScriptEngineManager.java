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
