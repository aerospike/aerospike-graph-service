package com.aerospike.firefly.feature;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import io.cucumber.java.Scenario;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.javatuples.Pair;
import org.junit.AssumptionViolatedException;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public abstract class FireflyWorldBase implements World {
    private static final String configLocation = "../conf/integration-test-settings-packed.properties";

    private static final String skipReasonOnCreate = "FireflyMergeEdgeStep always verify onCreate.";
    private static final String skipReasonErrorMessage = "Error message includes step name.";
    private static final String skipReasonMultiProperty = "Test does not assert correctly when default Vertex Property cardinality is not single.";
    private static final String skipCardinalitySet = "Set is not supported."; // TODO GRAPH-1565: Support VertexProperty.Cardinality.set

    private static final List<Pair<String, String>> skip = new ArrayList<>() {{
        add(Pair.with("g_mergeEXlabel_knows_out_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated", skipReasonOnCreate));
        add(Pair.with("g_mergeEXout_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated", skipReasonOnCreate));
        add(Pair.with("g_V_mergeEXlabel_knows_out_marko_in_vadasX_optionXonMatch_sideEffectXpropertyXweight_0XX_constantXemptyXX", skipReasonErrorMessage));
        add(Pair.with("g_withSideEffectXm_age_19X_V_hasXperson_name_markoX_mergeVXselectXcXX_optionXonMatch_sideEffectXpropertiesXageX_dropX_selectXmXX_option", skipReasonErrorMessage));
        add(Pair.with("g_V_hasXperson_name_aliceX_propertyXsingle_age_unionXage_constantX1XX_sumX", skipReasonMultiProperty));

        // TODO GRAPH-1565: Remove these from the skipped tests list
        add(Pair.with("g_mergeVXname_aliceX_optionXonCreate_age_setX81XX", skipCardinalitySet));
        add(Pair.with("g_mergeVXname_aliceX_optionXonCreate_age_singleX81X_age_81_setX", skipCardinalitySet));
        add(Pair.with("g_mergeVXname_markoX_optionXonMatch_age_setX31XX", skipCardinalitySet));
        add(Pair.with("g_mergeVXname_markoX_optionXonMatch_name_allen_age_setX31X_singleX", skipCardinalitySet));
        add(Pair.with("g_V_hasXname_fooX_propertyXname_setXbarX_age_43X", skipCardinalitySet));
        add(Pair.with("g_V_hasXname_fooX_propertyXset_name_bar_age_singleX43XX", skipCardinalitySet));
        add(Pair.with("g_mergeVXname_markoX_optionXonMatch_age_setX33XX", skipCardinalitySet));
    }};

    protected static Configuration getConfiguration(final String graphName, final boolean withMRT) {
        final Configuration config = ConfigurationHelper.loadFromFile(Path.of(configLocation));
        if (withMRT)
            config.setProperty(ConfigurationHelper.Keys.MRT_ENABLED_FLAG, "true");
        config.setProperty(ConfigurationHelper.Keys.GRAPH_ID, graphName);
        config.setProperty(ConfigurationHelper.Keys.TRAVERSAL_NAME, "g" + graphName);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_CARDINALITY, "list");

        return config;
    }

    protected static FireflyGraph createFireflyGraph(final String graphName, final Graph graph, final boolean withMRT) {
        final FireflyGraph firefly = FireflyGraph.open(getConfiguration(graphName, withMRT));
        firefly.getBaseGraph().dropDatabase(firefly, false);
        GraphHelper.cloneElements(graph, firefly);
        return firefly;
    }

    @Override
    public String changePathToDataFile(final String pathToFileFromGremlin) {
        return ".." + File.separator + pathToFileFromGremlin;
    }

    @Override
    public void beforeEachScenario(final Scenario scenario) {
        final Optional<Pair<String, String>> skipped = skip.stream().
                filter(s -> s.getValue0().equals(scenario.getName())).findFirst();
        if (skipped.isPresent())
            throw new AssumptionViolatedException(skipped.get().getValue1());
    }

    @Override
    public String convertIdToScript(final Object id, final Class<? extends Element> type) {
        if (Edge.class.isAssignableFrom(type))
            return "'" + id.toString() + "'";
        return id.toString();
    }
}
