package com.aerospike.firefly.olap.features;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Stage;
import io.cucumber.guice.CucumberModules;
import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.apache.tinkerpop.gremlin.features.AbstractGuiceFactory;
import org.apache.tinkerpop.gremlin.features.World;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        tags = "not @RemoteOnly and not @GraphComputerOnly and "+
                "not @AllowNullPropertyValues and not @GraphComputerVerificationElementSupported and "+
                "not @TinkerServiceRegistry and not @StepRead and not @MultiProperties and " +
                "not @UserSuppliedVertexPropertyIds and not @InsertionOrderingRequired and " +
                "not @UserSuppliedEdgeIds and " +
                // might need fix, but not easy with scans/index reads
                "not @WithPartitionStrategy",
        glue = { "org.apache.tinkerpop.gremlin.features" },
        objectFactory = FireflyComputerFeatureTest.FireflyComputerGraphGuiceFactory.class,
        features = { "classpath:/org/apache/tinkerpop/gremlin/test/features" },
        plugin = {"progress", "junit:target/cucumber.xml"})
public class FireflyComputerFeatureTest {
    public static class FireflyComputerGraphGuiceFactory extends AbstractGuiceFactory {
        public FireflyComputerGraphGuiceFactory() {
            super(Guice.createInjector(Stage.PRODUCTION, CucumberModules.createScenarioModule(), new ServiceModule()));
        }
    }

    public static final class ServiceModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(World.class).to(FireflyComputerWorld.class);
        }
    }
}
