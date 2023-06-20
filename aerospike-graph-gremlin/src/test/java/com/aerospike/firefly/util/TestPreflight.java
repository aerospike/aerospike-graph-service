package com.aerospike.firefly.util;

import org.junit.Test;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestPreflight {

    @Test
    public void testCheckJavaVersion(){
        PreflightCheckHelper.checkSupportedJVM();
    }
}
