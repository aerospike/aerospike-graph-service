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
        idManager = new NumericIdManager("TEST_COUNTER");
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
}
