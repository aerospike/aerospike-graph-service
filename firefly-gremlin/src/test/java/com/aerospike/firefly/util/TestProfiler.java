package com.aerospike.firefly.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestProfiler {
    @Test
    public void testCallCounter(){
        ProfileUtil.reset();
        ProfileUtil.increment(TestProfiler.class,"testCallCounter");
        ProfileUtil.increment(TestProfiler.class,"testCallCounter");
        ProfileUtil.increment(TestProfiler.class,"testCallCounter");
        assertEquals(3, ProfileUtil.metrics.get(TestProfiler.class.getName()).get("testCallCounter").get());
    }
    @Test
    public void testReset(){
        ProfileUtil.increment(TestProfiler.class,"testCallCounter");
        ProfileUtil.reset();
        assertTrue(ProfileUtil.metrics.isEmpty());
    }

    @Test
    public void testToString(){
        ProfileUtil.increment(TestProfiler.class,"testCallCounter");
        ProfileUtil.increment(String.class,"someStringFn");
        ProfileUtil.increment(String.class,"someStringFn");

        String output = ProfileUtil.report();
        System.out.println(output);
    }
}
