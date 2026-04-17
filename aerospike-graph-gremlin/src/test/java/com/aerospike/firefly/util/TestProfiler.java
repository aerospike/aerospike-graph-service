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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
