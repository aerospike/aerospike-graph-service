package com.aerospike.firefly.structure.id;

import com.aerospike.client.Value;
import com.aerospike.client.util.Crypto;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class FireflyPhatEdgeId extends FireflyIdPoly {
    private final long capacity;

    public FireflyPhatEdgeId(final long id, final long capacity, final String edgeSetName) {
        super(id, edgeSetName);
        this.capacity = capacity;
    }

    @Override
    public Object getStorageId() {
        return ((long) this.id) / capacity;
    }

    /**
     * Uses the Aerospike Client Crypto routines to produce a RIPEMD160 hash of the string capable of retrieving the record by digest.
     *
     * @param setName the Aerospike namespace
     * @return the digest of the string
     */
    @Override
    public byte[] getIdHash(String setName) {
        final Value keyValue = Value.get(this.getStorageId());
        return Crypto.computeDigest(setName, keyValue);
    }

    @Override
    public int hashCode() {
        return getUserId().hashCode();
    }

    @Override
    public int compareTo(final Object o) {
        if (o instanceof FireflyId) {
            Long userId = (Long) getUserId();
            return userId.compareTo((Long)((FireflyId) o).getUserId());
        } else {
            throw new IllegalArgumentException("Cannot compare FireflyId to " + o.getClass().getName());
        }
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && this.getUserId().equals(((FireflyId) o).getUserId());
    }
}
