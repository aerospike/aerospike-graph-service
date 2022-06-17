package com.aerospike.firefly.util;

import org.junit.jupiter.api.Test;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestPreflight {

    @Test
    void testCheckJavaVersion(){
       PreflightCheckHelper.checkSupportedJVM();
    }
}
