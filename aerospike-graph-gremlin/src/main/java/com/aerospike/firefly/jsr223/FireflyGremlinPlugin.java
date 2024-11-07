package com.aerospike.firefly.jsr223;

import com.aerospike.client.AerospikeException;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.process.computer.local.LocalGraphComputer;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyGraphFeatures;
import com.aerospike.firefly.structure.FireflyGraphVariables;
import com.aerospike.firefly.structure.FireflyProperty;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public final class FireflyGremlinPlugin extends AbstractGremlinPlugin {
    private static final String NAME = "aerospike.firefly";
    private static final ImportCustomizer imports;

    static {
        try {
            imports = DefaultImportCustomizer.build()
                    .addClassImports(
                            FireflyGraph.class,
                            FireflyGraphFeatures.class,
                            FireflyGraphVariables.class,
                            FireflyElement.class,
                            FireflyEdge.class,
                            FireflyVertex.class,
                            FireflyVertexProperty.class,
                            FireflyProperty.class,
                            FireflyHelper.class,
                            ConfigurationHelper.class,
                            AerospikeException.class,
                            AerospikeConnection.class,
                            LocalGraphComputer.class
                    ).create();
        } catch (Exception ex) {
            System.out.println("ERROR LOADING");
            throw new RuntimeException(ex);
        }
    }

    private static final FireflyGremlinPlugin instance = new FireflyGremlinPlugin();

    public FireflyGremlinPlugin() {
        super(NAME, imports);
    }

    public static GremlinPlugin instance() {
        return instance;
    }

    @Override
    public boolean requireRestart() {
        return true;
    }
}
