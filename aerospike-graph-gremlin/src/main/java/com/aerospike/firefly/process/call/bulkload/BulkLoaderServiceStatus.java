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

package com.aerospike.firefly.process.call.bulkload;

import com.aerospike.firefly.process.call.bulkload.utils.FireflyBulkLoaderInterface;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Map;

public class BulkLoaderServiceStatus<I> extends BulkLoaderServiceBase<I, Map<String, Object>> {

    public BulkLoaderServiceStatus(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminServiceName() {
        return "status";
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to '%s'.\n" +
                        "\tExpected no arguments.\n" +
                        "\tProvided arguments: '%s'.\n" +
                        "\tExample of correct usage:\n" +
                        "\t\tg.call(\"%s\").next();\n",
                getName(), params, getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        // Should be no parameters.
        return params.isEmpty();
    }

    @Override
    protected Map<String, Object> execute(final Map params) {
        try {
            final Class<? extends FireflyBulkLoaderInterface> bulkLoaderClass = (Class<? extends FireflyBulkLoaderInterface>)
                    Class.forName("com.aerospike.firefly.bulkloader.SparkBulkLoaderMain");
            final Map<String, Object> status = bulkLoaderClass.newInstance().getStatus();
            return status;
        } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            throw new IllegalStateException("ERROR: To use the bulk loader via the call API, " +
                    "use the docker image with bulk loader support.", e);
        }
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("[{}] - {} - Get bulk load status.", getUser(), getName());
    }

    @Override
    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.READ;
    }
}
