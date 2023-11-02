package com.aerospike.firefly.util;

import com.aerospike.firefly.runtime.PluginInterface;
import org.apache.commons.configuration2.Configuration;

import java.util.Optional;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class PluginUtil {
    public static final String STATIC_OPEN_METHOD = "open";

    /**
     * Opens a class reference using the static open(Configuration) method
     * @param className The class to open
     * @param config The Configuration argument to pass to the open method
     * @return a new class instance
     */
    private static Object openClassRef(final String className, final Configuration config) {
        try {
            final Class clazz = Optional.ofNullable(Class.forName(className)).orElseThrow(() ->
                    new RuntimeException("Could not load class: " + className));
            return clazz.getMethod(STATIC_OPEN_METHOD, Configuration.class).invoke(null, config);
        } catch (final NoSuchMethodException nsm) {
            throw new RuntimeException(String.format("Class %s does not properly implement a static open(Configuration) method", className));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Loads a plugin into a system
     * @param pluginClass The plugin class to load
     * @param config The configuration to pass to the plugin
     * @param system The system to load the plugin into
     */
    public static void loadPlugin(final String pluginClass, final Configuration config, final Object system) {
        final PluginInterface pluginImpl = (PluginInterface) openClassRef(pluginClass, null);
        try {
            pluginImpl.getClass().getMethod(PluginInterface.Methods.PLUG_INTO, Object.class).invoke(pluginImpl, system);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}
