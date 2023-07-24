package com.aerospike.firefly.util;

import org.apache.commons.configuration2.Configuration;

import java.util.Optional;

/*
  Created by Grant Haywood grant@iowntheinter.net
  7/23/23
*/
public class PluginUtil {
    public static final String STATIC_OPEN_METHOD = "open";

    private static Object openClassRef(final String className, final Configuration config) {
        try {
            final Class clazz = Optional.ofNullable(Class.forName(className)).orElseThrow(() ->
                    new RuntimeException("Could not load class: " + className));
            return clazz.getMethod(STATIC_OPEN_METHOD, Configuration.class).invoke(null, config);
        } catch (NoSuchMethodException nsm) {
            throw new RuntimeException(String.format("Class %s does not properly implement a static open(Configuration) method", className));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void loadPlugin(final String pluginClass, final Configuration config, final Object system) {
        PluginInterface pluginImpl = (PluginInterface) openClassRef(pluginClass, null);
        try {
            pluginImpl.getClass().getMethod(PluginInterface.Methods.PLUG_INTO, Object.class).invoke(pluginImpl, system);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
