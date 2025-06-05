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
