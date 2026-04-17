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

package com.aerospike.firefly.util;

import com.aerospike.firefly.runtime.PluginInterface;
import org.apache.commons.configuration2.Configuration;

import java.util.Optional;

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
        final Object pluginImpl = openClassRef(pluginClass, config);
        try {
            pluginImpl.getClass().getMethod(PluginInterface.Methods.PLUG_INTO, Object.class).invoke(pluginImpl, system);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}
