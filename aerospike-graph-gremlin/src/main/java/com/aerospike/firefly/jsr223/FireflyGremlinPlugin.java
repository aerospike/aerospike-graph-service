package com.aerospike.firefly.jsr223;

import com.aerospike.client.AerospikeException;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.*;
import com.aerospike.firefly.structure.util.FireflyHelper;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.HealthcheckServer;
import com.aerospike.firefly.util.PrometheusMetricsServer;
import org.apache.commons.configuration2.Configuration;
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
    public static final PrometheusMetricsServer metricsProtocolServer = PrometheusMetricsServer.create(
            PrometheusMetricsServer.DEFAULT_PROMETHEUS_PORT,
            PrometheusMetricsServer.DEFAULT_PROMETHEUS_PATH);
    public static void startHealthcheckServer(final Configuration config, final int port){
        HealthcheckServer.create(config, port).start();
    }

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
