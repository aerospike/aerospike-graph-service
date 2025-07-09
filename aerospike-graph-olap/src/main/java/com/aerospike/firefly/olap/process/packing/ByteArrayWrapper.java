package com.aerospike.firefly.olap.process.packing;

import java.util.Arrays;

public class ByteArrayWrapper {
    private final byte[] data;
    private Integer hashCode = null;

    public ByteArrayWrapper(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ByteArrayWrapper)) return false;
        return Arrays.equals(data, ((ByteArrayWrapper) o).data);
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = Arrays.hashCode(data);
        }

        return hashCode;
    }
}
