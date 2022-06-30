package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyVertex;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class NumericIdManagerTest {
    private IdManager<Long> idManager;

    @Before
    public void createIdManager() {
        // Regenerate each time so it is cleared.
        idManager = new NumericIdManager<>(FireflyVertex.class, "TEST_COUNTER");
    }

    @Test
    public void testAllow() {
        // Supported classes.
        assertTrue(idManager.allow(String.class));
        assertTrue(idManager.allow(Long.class));
        assertTrue(idManager.allow(Integer.class));
        assertTrue(idManager.allow(Double.class));

        // Some random unsupported classes.
        assertFalse(idManager.allow(FireflyVertex.class));
        assertFalse(idManager.allow(NumericIdManager.class));

    }

    @Test
    public void testConvert() {
        // Conversion to Long from valid Long String or int / long primitives should work.
        assertEquals(1L, NumericIdManager.convert("1").longValue());
        assertEquals(1L, NumericIdManager.convert(1).longValue());
        assertEquals(1L, NumericIdManager.convert(1L).longValue());

        // Invalid Strings or otherwise invalid types should throw an IllegalArgumentException.
        assertThrows(IllegalArgumentException.class, () -> NumericIdManager.convert("a"));
        assertThrows(IllegalArgumentException.class, () -> NumericIdManager.convert(new NumericIdManager<>(FireflyVertex.class, "")));
    }
}
