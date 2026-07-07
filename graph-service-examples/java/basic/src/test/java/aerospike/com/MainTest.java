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

package aerospike.com;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class MainTest {

    @Test
    public void testFullAppFlow() {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final PrintStream originalOut = System.out;
        try {
            System.setOut(new PrintStream(outputStream));

            Main.main(new String[]{});

            System.setOut(originalOut);
            final String output = outputStream.toString();

            assertTrue(output.contains("Connected to Aerospike Graph Service; Adding Data..."));
            assertTrue(output.contains("Data written successfully..."));
            assertTrue(output.contains("QUERY 1: Transactions initiated by Alice:"));
            assertTrue(output.contains("Dropping Dataset."));
            assertTrue(output.contains("Closing Connection..."));
        } finally {
            System.setOut(originalOut);
        }
    }
} 