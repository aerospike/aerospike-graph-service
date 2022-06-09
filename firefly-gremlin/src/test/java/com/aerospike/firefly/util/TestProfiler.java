package com.aerospike.firefly.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestProfiler {
    @Test
    public void testCallCounter(){
        Profile.reset();
        Profile.increment(TestProfiler.class,"testCallCounter");
        Profile.increment(TestProfiler.class,"testCallCounter");
        Profile.increment(TestProfiler.class,"testCallCounter");
        assertEquals(3,Profile.metrics.get(TestProfiler.class.getName()).get("testCallCounter").get());
    }
    @Test
    public void testReset(){
        Profile.increment(TestProfiler.class,"testCallCounter");
        Profile.reset();
        assertTrue(Profile.metrics.isEmpty());
    }

    @Test
    public void testToString(){
        Profile.increment(TestProfiler.class,"testCallCounter");
        Profile.increment(String.class,"someStringFn");
        Profile.increment(String.class,"someStringFn");

        String output = Profile.report();
        System.out.println(output);
    }
}
