package com.aerospike.firefly.util;/*
  Created by Grant Haywood grant@iowntheinter.net
  7/23/23
*/

import java.util.List;
import java.util.Map;

public interface PluginInterface {
    public static class Methods {
        public static String API = "api";
        public static String PLUG_INTO = "plugInto";
    }

    Map<String, List<String>> api();

    void plugInto(Object system);
}