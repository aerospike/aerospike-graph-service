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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class OutputCapturer extends ByteArrayOutputStream implements AutoCloseable {
    private final PrintStream originalOut;

    public OutputCapturer() {
        super();
        originalOut = System.out;
        System.setOut(new PrintStream(this));
    }

    @Override
    public synchronized void write(byte b[], int off, int len) {
        super.write(b, off, len);
        originalOut.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
        super.close();
        System.setOut(originalOut);
    }

    public String[] getLines() {
        return toString().split("\n");
    }
}
