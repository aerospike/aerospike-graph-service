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

package com.aerospike.firefly.feature;

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
        tags = "not @RemoteOnly and not @GraphComputerOnly and not @AllowNullPropertyValues and "+
                "not @TinkerServiceRegistry and not @StepRead and " +
                "not @UserSuppliedVertexPropertyIds and not @InsertionOrderingRequired and " +
                "not @UserSuppliedEdgeIds and " +
                // might need fix, but not easy with scans/index reads
                "not @WithPartitionStrategy",
        glue = { "org.apache.tinkerpop.gremlin.features" },
        objectFactory = FireflyFeatureTest.FireflyGraphGuiceFactory.class,
        features = { "classpath:/org/apache/tinkerpop/gremlin/test/features" },
        plugin = {"progress", "junit:target/cucumber.xml"})
public class FireflyFeatureTest {
    public static class FireflyGraphGuiceFactory extends AbstractGuiceFactory {
        public FireflyGraphGuiceFactory() {
            super(Guice.createInjector(Stage.PRODUCTION, CucumberModules.createScenarioModule(), new ServiceModule()));
        }
    }

    public static final class ServiceModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(World.class).to(FireflyWorld.class);
        }
    }
}
