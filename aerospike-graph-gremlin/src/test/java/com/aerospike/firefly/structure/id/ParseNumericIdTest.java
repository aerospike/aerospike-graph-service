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

package com.aerospike.firefly.structure.id;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.Random;

public class ParseNumericIdTest {

    @Test
    public void testNumericIds() {
        Assert.assertEquals(Long.valueOf(0L), FireflyIdFactory.parseNumericId("0"));
        Assert.assertEquals(Long.valueOf(0L), FireflyIdFactory.parseNumericId("-0"));
        Assert.assertEquals(Long.valueOf(0L), FireflyIdFactory.parseNumericId("+0"));
        Assert.assertEquals(Long.valueOf(42L), FireflyIdFactory.parseNumericId("42"));
        Assert.assertEquals(Long.valueOf(42L), FireflyIdFactory.parseNumericId("+42"));
        Assert.assertEquals(Long.valueOf(-42L), FireflyIdFactory.parseNumericId("-42"));
        Assert.assertEquals(Long.valueOf(7L), FireflyIdFactory.parseNumericId("007"));
        Assert.assertEquals(Long.valueOf(Long.MAX_VALUE), FireflyIdFactory.parseNumericId("9223372036854775807"));
        Assert.assertEquals(Long.valueOf(Long.MAX_VALUE), FireflyIdFactory.parseNumericId("+9223372036854775807"));
        Assert.assertEquals(Long.valueOf(Long.MIN_VALUE), FireflyIdFactory.parseNumericId("-9223372036854775808"));
        Assert.assertEquals(Long.valueOf(Long.MAX_VALUE), FireflyIdFactory.parseNumericId("0009223372036854775807"));
    }

    @Test
    public void testNonNumericIds() {
        for (final String id : List.of("", "-", "+", "+-5", "-+5", "--5", "++5", " 5", "5 ", "5.0", "1e3", "0x10",
                "alice", "12a", "\u0664\u0662")) {
            Assert.assertNull(id, FireflyIdFactory.parseNumericId(id));
        }
    }

    @Test
    public void testOutOfRangeNumericIds() {
        for (final String id : List.of("9223372036854775808", "+9223372036854775808", "-9223372036854775809",
                "99999999999999999999", "1997231212526362784799127961188217")) {
            Assert.assertNull(id, FireflyIdFactory.parseNumericId(id));
        }
    }

    @Test
    public void testMatchesLongParseLong() {
        final Random random = new Random(42);
        for (int i = 0; i < 100_000; i++) {
            final BigInteger value = new BigInteger(1 + random.nextInt(70), random)
                    .subtract(BigInteger.ONE.shiftLeft(random.nextInt(70)));
            final String sign = random.nextInt(4) == 0 ? "+" : "";
            final String id = value.signum() < 0 ? value.toString() : sign + value;
            Assert.assertEquals(id, parseLongOrNull(id), FireflyIdFactory.parseNumericId(id));
        }
    }

    private static Long parseLongOrNull(final String id) {
        try {
            return Long.parseLong(id);
        } catch (final NumberFormatException e) {
            return null;
        }
    }
}
