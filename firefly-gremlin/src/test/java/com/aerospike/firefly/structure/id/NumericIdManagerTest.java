package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyVertex;
import org.junit.Before;
import org.junit.Test;

import static com.aerospike.firefly.util.TestUtil.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        assertEquals(1L, idManager.convert("1").longValue());
        assertEquals(1L, idManager.convert(1).longValue());
        assertEquals(1L, idManager.convert(1L).longValue());

        // Invalid Strings or otherwise invalid types should throw an IllegalArgumentException.
        assertThrows(IllegalArgumentException.class, () -> idManager.convert("a"));
        assertThrows(IllegalArgumentException.class, () -> idManager.convert(new NumericIdManager<>(FireflyVertex.class, "")));
    }

    @Test
    public void testAddToCache() {
        // Expect to only be able to add same id once, even if type changes.
        idManager.addToCache(1L);
        assertThrows(IllegalArgumentException.class, () -> idManager.addToCache(1L));
        assertThrows(IllegalArgumentException.class, () -> idManager.addToCache("1"));

        // Should be able to add an additional item.
        idManager.addToCache(2L);
    }

    @Test
    public void testRemoveFromCache() {
        // Expect UnsupportedOperationException to be thown if item is not in cache.
        assertThrows(AssertionError.class, () -> idManager.removeFromCache(1L));

        // Expect things to work if item is in cache. Even if we use different types.
        idManager.addToCache(1L);
        idManager.addToCache("2");
        idManager.addToCache(3);
        idManager.removeFromCache("1");
        idManager.removeFromCache(2);
        idManager.removeFromCache(3L);
    }
}
