package com.aerospike.firefly.olap.features;


import io.cucumber.java.Scenario;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.javatuples.Pair;
import org.junit.AssumptionViolatedException;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class FireflyRemoteComputerWorld implements World {
    private final Cluster cluster;

    private static final List<String> TAGS_TO_IGNORE = Arrays.asList(
            "@StepDrop",
            "@StepInject",
            "@StepV",
            "@StepE",
            "@GraphComputerVerificationOneBulk",
            "@GraphComputerVerificationStrategyNotSupported",
            "@GraphComputerVerificationMidVNotSupported",
            "@GraphComputerVerificationInjectionNotSupported",
            "@GraphComputerVerificationStarGraphExceeded",
            "@GraphComputerVerificationReferenceOnly",
            "@TinkerServiceRegistry");

    private static final List<Pair<String, String>> SKIP_TESTS = new ArrayList<>();

    public static final int PORT = 45940;
    public static final String ADDRESS = "localhost";

    public FireflyRemoteComputerWorld() {
        cluster = Cluster.build(ADDRESS).port(PORT).create();
    }

    @Override
    public GraphTraversalSource getGraphTraversalSource(final LoadGraphWith.GraphData graphData) {
        final Client client = cluster.connect();
        final String remoteTraversalSource;

        switch (graphData) {
            case CLASSIC:
                remoteTraversalSource = "gclassic";
                break;
            case CREW:
                remoteTraversalSource = "gcrew";
                break;
            case MODERN:
                remoteTraversalSource = "gmodern";
                break;
            case SINK:
                remoteTraversalSource = "gsink";
                break;
            case GRATEFUL:
                remoteTraversalSource = "ggrateful";
                break;
            default:
                throw new UnsupportedOperationException("GraphData not supported: " + graphData.name());
        }

        return traversal().withRemote(DriverRemoteConnection.using(client, remoteTraversalSource));
    }

    @Override
    public String changePathToDataFile(final String pathToFileFromGremlin) {
        return ".." + File.separator + pathToFileFromGremlin;
    }

    @Override
    public void beforeEachScenario(final Scenario scenario) {
        final List<String> ignores = TAGS_TO_IGNORE.stream().filter(t -> scenario.getSourceTagNames().contains(t)).collect(Collectors.toList());
        if (!ignores.isEmpty())
            throw new AssumptionViolatedException(String.format("This scenario is not supported with GraphComputer: %s", ignores));

        final Optional<Pair<String, String>> skipped = SKIP_TESTS.stream().
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
