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

package com.aerospike.firefly.process.call.metadata;

import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Map;

public class MetadataServiceSetConfig<I, R> extends MetadataServiceBase<I, R> {

    public MetadataServiceSetConfig(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminServiceName() {
        return "set-config";
    }

    @Override
    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.ADMIN;
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to '%s'.\n" +
                        "\tExpected non-empty arguments map.\n" +
                        "\tExamples of correct usage:\n" +
                        "\t\tg.call(\"%s\").with(\"aerospike.client.policy.write.socketTimeout\", \"10000\").next();\n",
                getName(), getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        return !params.isEmpty();
    }

    @Override
    protected R execute(final Map params) {
        graph.getBaseGraph().updateConfiguration(params);

        return (R) ("Successfully updated configuration " + params + ".");
    }

    @Override
    public Map<String, String> describeParams() {
        return Map.of();
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("[{}] - {} - Changed graph configuration: ", getUser(), getName(), params);
    }
}
