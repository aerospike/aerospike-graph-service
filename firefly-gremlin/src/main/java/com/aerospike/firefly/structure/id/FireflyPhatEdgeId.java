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
        // TODO GRAPH-426: Won't need this. See JIRA for details.
        // The IdManager always generates the first Id as -1, so offset by 1 in order to ensure that the phat edge 0
        // has the correct number of edges added to it.
        // Example: phat edge with capacity 2 would never be properly filled without this logic since it would only
        // contain edge with Id -1 since the next generated edge with Id -2 will store in phat edge -1.
        return ((long) this.id + 1) / capacity;
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
