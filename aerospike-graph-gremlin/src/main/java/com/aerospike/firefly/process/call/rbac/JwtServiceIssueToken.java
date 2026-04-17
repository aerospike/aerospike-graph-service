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

package com.aerospike.firefly.process.call.rbac;

import com.aerospike.firefly.security.JWTAuthenticator;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.HashMap;
import java.util.Map;

import static com.aerospike.firefly.process.traversal.strategy.optimization.FireflyAuthenticationStrategy.*;
import static com.aerospike.firefly.security.UserContext.ROLE;

public class JwtServiceIssueToken<I, R> extends JwtServiceBase<I, R> {

    public JwtServiceIssueToken(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    public String getAdminServiceName() {
        return "issue-token";
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to '%s'.\n" +
                        "\tExpected arguments: 'username', 'role'.\n" +
                        "\tAcceptable values of role are: 'READ', 'READ_WRITE', 'ADMIN' or Map of pairs GraphID-role.\n" +
                        "\tProvided arguments: '%s'.\n" +
                        "\tExample of correct usage:\n" +
                        "\t\tg.call(\"%s\").with(\"username\", \"alice\").with(\"role\", \"ADMIN\").next();\n" +
                        "\t\tg.call(\"%s\").with(\"username\", \"alice\").with(\"Graph1\", \"ADMIN\").with(\"Graph2\", \"READ\").next();\n" +
                        "\t\tg.call(\"%s\").with(\"username\", \"alice\").with(\"role\", [\"Graph1\": \"ADMIN\"]).next();\n" +
                        "\tor to set a token that expires in 1 day:\n" +
                        "\t\tg.call(\"%s\").with(\"username\", \"alice\").with(\"role\", \"ADMIN\").with(\"expiry\", 24 * 60 * 60).next();",
                getName(), params, getName(), getName(), getName(), getName());
    }

    @Override
    public Map<String, String> describeParams() {
        final Map<String, String> parameters = new HashMap<>();
        parameters.put("username", "The username to issue the token for.");
        parameters.put("role", "The role to issue the token for. Acceptable values are 'READ', 'READ_WRITE', 'ADMIN' or Map with pairs GraphID-role.");
        parameters.put("expiry", "The expiry time for the token in seconds from current time. Optional parameter.");
        return parameters;
    }

    /*
        for http requests expected input is in format `username=Alice&role=READ` for global roles
        or per graph `username=Alice&graphID1=READ&graphID2=ADMIN`,
        but not both
    */
    private void extractRoles(final Map params) {
        final Map<String, String> additionalParams = new HashMap<>();
        for (final Map.Entry<String, String> p : ((Map<String, String>) params).entrySet()) {
            if (!p.getKey().equals("username") && !p.getKey().equals("role") && !p.getKey().equals("expiry")) {
                additionalParams.put(p.getKey(), p.getValue());
            }
        }

        if (!additionalParams.isEmpty()) {
            if (params.containsKey("role")) {
                throw new IllegalStateException("Cannot issue JWT token. The role must be global or assigned per graph.");
            }
            params.put("role", additionalParams);
            additionalParams.forEach((k, v) -> params.remove(k));
        }
    }

    // for HTTP we call sanitize, then execute
    // for call we first isValidPermissions, then sanitize
    @Override
    protected boolean sanitize(final Map params) {
        extractRoles(params);

        if (!params.containsKey("username") ||
                !params.containsKey("role") ||
                !params.get("username").getClass().equals(String.class) ||
                (!params.get("role").getClass().equals(String.class) && !(params.get("role") instanceof Map))) {
            return false;
        }

        final String username = (String) params.get("username");
        if (username == null || username.isEmpty()) {
            return false;
        }

        if (params.get("role").getClass().equals(String.class)) {
            final String role = (String) params.get("role");
            if (role == null || role.isEmpty() || !isValidRole(role)) {
                return false;
            }
        } else {
            try {
                final Map<String, String> map = (Map<String, String>) params.get("role");
                if (map.isEmpty()) {
                    return false;
                }
                for (final String value : map.values()) {
                    if (!isValidRole(value)) {
                        return false;
                    }
                }
            } catch (final ClassCastException e) {
                return false;
            }
        }

        if (params.get("expiry") != null &&
                !Number.class.isAssignableFrom(params.get("expiry").getClass())) {
            return false;
        }

        return true;
    }

    private boolean isValidRole(final String role) {
        return role.equals("READ") || role.equals("READ_WRITE") || role.equals("ADMIN");
    }

    @Override
    protected R execute(final Map params) {
        final String username = (String) params.get("username");
        final Object role = params.get("role");
        final Number expiry = (Number) params.get("expiry");

        final JWTAuthenticator jwtAuthenticator = JWTAuthenticator.getInstance();
        if (jwtAuthenticator == null) {
            throw new IllegalStateException("Cannot issue JWT token because " +
                    "JWT authentication is not enabled on this Aerospike Graph instance.");
        }
        return (R) jwtAuthenticator.createToken(username, role, expiry);
    }

    @Override
    protected void auditLog(final Map params) {
        final String username = (String) params.get("username");
        LOGGER.info("[{}] - {} - Creating a new JWT token for user '{}'.", getUser(), getName(), username);
    }

    // not for HTTP
    @Override
    protected boolean isValidPermissions(final Map params, final UserClaims userContext) {
        extractRoles(params);

        final Object role = params.get("role");
        if (!(role instanceof Map)) {
            return true;
        }

        final Map<String, String> requestedRoles = (Map<String, String>) params.get("role");
        for (final String requested : requestedRoles.keySet()) {
            final ROLE graphRole = userContext.getRole(requested);
            if (!ROLE.ADMIN.equals(graphRole)) {
                return false;
            }
        }
        return true;
    }
}
