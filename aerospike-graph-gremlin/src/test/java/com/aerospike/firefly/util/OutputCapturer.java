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
