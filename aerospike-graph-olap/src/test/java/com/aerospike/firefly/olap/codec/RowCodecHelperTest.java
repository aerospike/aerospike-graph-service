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

package com.aerospike.firefly.olap.codec;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.olap.codec.RowCodecHelper.deserialize;
import static com.aerospike.firefly.olap.codec.RowCodecHelper.serialize;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class RowCodecHelperTest {

    @Parameterized.Parameter
    public Object object;

    @Parameterized.Parameters()
    public static Iterable<Object> generateTestParameters() {
        return Arrays.asList(1, "Test", List.of(1, 2L), Map.of("key", 0));
    }

    @Test
    public void test() {
        assertEquals(object, deserialize(serialize(object)));
    }
}
