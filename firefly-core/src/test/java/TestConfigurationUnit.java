import com.aerospike.firefly.util.FireflyConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestConfigurationUnit {


    @Test
    void testLoadConfigurationFromResources() {
        final FireflyConfiguration c = FireflyConfiguration.loadFromResources("phaseshift-integration-settings.properties");
        assertEquals(c.aerospikeHost(), "aerospike-dev.phaseshift.internal");
        assertEquals(c.aerospikePort(), 3000);
    }

    @Test
    void testLoadConfigurationFromFile() {

    }

    @Test
    void testLoadConfigurationFromEnv() {

    }
}
