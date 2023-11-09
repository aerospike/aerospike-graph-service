package com.aerospike.firefly.sizing;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class SizingToolPlugin {
    private SizingToolPlugin() {
    }

    // Configuration is unused.
    public static SizingToolPlugin open(final Configuration config) {
        final SizingToolPlugin plugin = new SizingToolPlugin();
        return plugin;
    }


    public void plugInto(final Object system) {
        if (!Graph.class.isAssignableFrom(system.getClass())) {
            throw new RuntimeException("Error, plugin can only be loaded into a Graph instance.");
        }
        final Graph graph = (Graph) system;
        final SizingToolServiceFactory<?, ?> sizingToolServiceFactory = new SizingToolServiceFactory<>();
        graph.getServiceRegistry().registerService(sizingToolServiceFactory);
    }
}
