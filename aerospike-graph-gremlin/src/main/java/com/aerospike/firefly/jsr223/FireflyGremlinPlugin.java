package com.aerospike.firefly.jsr223;

import com.aerospike.client.AerospikeException;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyGraphFeatures;
import com.aerospike.firefly.structure.FireflyGraphVariables;
import com.aerospike.firefly.structure.FireflyProperty;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.util.FireflyHelper;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.runtime.HttpServer;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.apache.tinkerpop.gremlin.server.Settings;

import java.io.File;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public final class FireflyGremlinPlugin extends AbstractGremlinPlugin {
    private static final String NAME = "aerospike.firefly";
    private static final ImportCustomizer imports;
    public static final HttpServer metricsProtocolServer;

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
                            AerospikeConnection.class
                    ).create();
        } catch (Exception ex) {
            System.out.println("ERROR LOADING");
            throw new RuntimeException(ex);
        }

        int httpPort = HttpServer.DEFAULT_HTTP_PORT;
        String prometheusPath = HttpServer.DEFAULT_PROMETHEUS_PATH;
        String healthcheckPath = HttpServer.DEFAULT_HEALTHCHECK_PATH;
        try {
            final Settings settings = FireflyGraph.getGremlinServerSettings();
            final String configFileLocation = settings.graphs.get("graph");
            if (configFileLocation != null) {
                if (new File(configFileLocation).exists()) {
                    final Configuration configuration = ConfigurationHelper.loadFromFile(configFileLocation);
                    httpPort = Integer.parseInt(ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.HTTP_PORT, configuration));
                    prometheusPath = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.PROMETHEUS_PATH, configuration);
                    healthcheckPath = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.HEALTHCHECK_PATH, configuration);
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        metricsProtocolServer = HttpServer.create(httpPort, prometheusPath, healthcheckPath);
    }

    public static void initializeGraphMetrics(final FireflyGraph fireflyGraph) {
        // NOTE: Invoking this function causes the HTTP server to start due to static initialization.
        HttpServer.registerGraphMetrics(fireflyGraph.getBaseGraph());
        HttpServer.registerHealthcheck(fireflyGraph);
    }

    private static final FireflyGremlinPlugin instance = new FireflyGremlinPlugin();

    public FireflyGremlinPlugin() {
        super(NAME, imports);
        metricsProtocolServer.start();
    }

    public static GremlinPlugin instance() {
        return instance;
    }

    @Override
    public boolean requireRestart() {
        return true;
    }
}
