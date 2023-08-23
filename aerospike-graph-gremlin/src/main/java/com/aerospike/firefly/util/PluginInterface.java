package com.aerospike.firefly.util;


import java.util.List;
import java.util.Map;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface PluginInterface {
    public static class Methods {
        public static String API = "api";
        public static String PLUG_INTO = "plugInto";
    }

    Map<String, List<String>> api();

    void plugInto(Object system);
}