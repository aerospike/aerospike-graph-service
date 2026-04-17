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

import com.aerospike.client.AerospikeClient;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

public class DiagnosticUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DiagnosticUtil.class);

    public static AerospikeClient enableWriteFails(final AerospikeClient aerospikeClient, final Configuration conf) {
        try {
            final double clientFailureRate = Double.valueOf(ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.CLIENT_FAILURE_RATE, conf));
            final Class clientFailureClass = Class.forName("com.aerospike.firefly.bulkloader.integration.util.FailingAerospikeClient");
            final Method method = clientFailureClass.getMethod("clientWithWriteFails", AerospikeClient.class, double.class);
            return (AerospikeClient) method.invoke(null, aerospikeClient, clientFailureRate);
        } catch (final Exception e) {
            LOG.error("Error instantiating failure client for testing", e);
            throw new RuntimeException(e);
        }

    }
}
