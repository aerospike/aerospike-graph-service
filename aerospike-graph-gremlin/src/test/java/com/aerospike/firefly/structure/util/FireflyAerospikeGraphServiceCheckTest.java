package com.aerospike.firefly.structure.util;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.Assert;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.fail;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyAerospikeGraphServiceCheckTest {
    // This test should only be called with a different feature-key file that does not include the graph-service.
    // in the standard test suite.
    // mvn test -pl aerospike-graph-gremlin -Dtest=FireflyAerospikeGraphServiceCheckTest -DfailIfNoTests=false -Dintegration.test.properties=packed --no-transfer-progress

    @Test
    public void testConnectFails() {
        final Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try {
            AerospikeConnection.connect(conf);
            fail("Connecting to Aerospike without graph-service support should have thrown an exception");
        } catch (final RuntimeException e) {
            Assert.assertEquals("Failed to initialize graph-service due to missing feature-key. " +
                    "Please ensure you're licensed for graph-service", e.getMessage());
        }
    }

    @Test
    public void testFeatureKeyFile() {
        Assert.assertThrows(RuntimeException.class, () -> FireflyAerospikeGraphServiceCheck.validateInfoResponse(""));
        Assert.assertThrows(RuntimeException.class, () -> FireflyAerospikeGraphServiceCheck.validateInfoResponse(null));
        Assert.assertThrows(RuntimeException.class, () -> FireflyAerospikeGraphServiceCheck.validateInfoResponse("feature-key"));
        Assert.assertThrows(RuntimeException.class, () -> FireflyAerospikeGraphServiceCheck.validateInfoResponse("feature-key;graph-service=false"));
        Assert.assertThrows(RuntimeException.class, () -> FireflyAerospikeGraphServiceCheck.validateInfoResponse("feature-key;graph-service"));
        Assert.assertThrows(RuntimeException.class, () -> FireflyAerospikeGraphServiceCheck.validateInfoResponse("feature-key;graph-service="));
        Assert.assertThrows(RuntimeException.class, () -> FireflyAerospikeGraphServiceCheck.validateInfoResponse("feature-key-version=2;serial-number=976250543;account-name=Aerospike_TEST_;account-ID=core_eng_Testing;valid-until-date=2024-01-15;asdb-change-notification=true;asdb-cluster-nodes-limit=0;asdb-compression=true;asdb-encryption-at-rest=true;asdb-flash-index=true;asdb-ldap=true;asdb-pmem=true;asdb-rack-aware=true;asdb-strong-consistency=true;asdb-vault=true;asdb-xdr=true;elasticsearch-connector=true;gpubsub-connector=true;mesg-jms-connector=true;mesg-kafka-connector=true;point-in-time-recovery=true;presto-connector=true;pulsar-connector=true;raf-realtime-analysis-framework=true;spark-connector=true"));
        Assert.assertThrows(RuntimeException.class, () -> FireflyAerospikeGraphServiceCheck.validateInfoResponse("feature-key-version=2;serial-number=976250543;account-name=Aerospike_TEST_;account-ID=core_eng_Testing;valid-until-date=2024-01-15;asdb-change-notification=true;asdb-cluster-nodes-limit=0;asdb-compression=true;asdb-encryption-at-rest=true;asdb-flash-index=true;asdb-ldap=true;asdb-pmem=true;asdb-rack-aware=true;asdb-strong-consistency=true;asdb-vault=true;asdb-xdr=true;elasticsearch-connector=true;gpubsub-connector=true;mesg-jms-connector=true;mesg-kafka-connector=true;point-in-time-recovery=true;presto-connector=true;pulsar-connector=true;raf-realtime-analysis-framework=true;spark-connector=true;graph-service=false"));
        Assert.assertThrows(RuntimeException.class, () -> FireflyAerospikeGraphServiceCheck.validateInfoResponse("feature-key-version=2;serial-number=976250543;account-name=Aerospike_TEST_;account-ID=core_eng_Testing;valid-until-date=2024-01-15;asdb-change-notification=true;asdb-cluster-nodes-limit=0;asdb-compression=true;asdb-encryption-at-rest=true;asdb-flash-index=true;asdb-ldap=true;asdb-pmem=true;asdb-rack-aware=true;asdb-strong-consistency=true;asdb-vault=true;asdb-xdr=true;elasticsearch-connector=true;gpubsub-connector=true;mesg-jms-connector=true;mesg-kafka-connector=true;point-in-time-recovery=true;presto-connector=true;pulsar-connector=true;raf-realtime-analysis-framework=true;spark-connector=true;graph-service"));
        Assert.assertThrows(RuntimeException.class, () -> FireflyAerospikeGraphServiceCheck.validateInfoResponse("feature-key-version=2;serial-number=976250543;account-name=Aerospike_TEST_;account-ID=core_eng_Testing;valid-until-date=2024-01-15;asdb-change-notification=true;asdb-cluster-nodes-limit=0;asdb-compression=true;asdb-encryption-at-rest=true;asdb-flash-index=true;asdb-ldap=true;asdb-pmem=true;asdb-rack-aware=true;asdb-strong-consistency=true;asdb-vault=true;asdb-xdr=true;elasticsearch-connector=true;gpubsub-connector=true;mesg-jms-connector=true;mesg-kafka-connector=true;point-in-time-recovery=true;presto-connector=true;pulsar-connector=true;raf-realtime-analysis-framework=true;spark-connector=true;graph-service="));
        FireflyAerospikeGraphServiceCheck.validateInfoResponse("graph-service=true");
        FireflyAerospikeGraphServiceCheck.validateInfoResponse("feature-key;graph-service=true");
        FireflyAerospikeGraphServiceCheck.validateInfoResponse("feature-key-version=2;serial-number=976250543;account-name=Aerospike_TEST_;account-ID=core_eng_Testing;valid-until-date=2024-01-15;asdb-change-notification=true;asdb-cluster-nodes-limit=0;asdb-compression=true;asdb-encryption-at-rest=true;asdb-flash-index=true;asdb-ldap=true;asdb-pmem=true;asdb-rack-aware=true;asdb-strong-consistency=true;asdb-vault=true;asdb-xdr=true;elasticsearch-connector=true;gpubsub-connector=true;mesg-jms-connector=true;mesg-kafka-connector=true;point-in-time-recovery=true;presto-connector=true;pulsar-connector=true;raf-realtime-analysis-framework=true;spark-connector=true;graph-service=true");
    }
}
