package com.aerospike.firefly.process.call.rbac;

import com.aerospike.firefly.security.JWTAuthenticator;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.HashMap;
import java.util.Map;

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
                        "\tAcceptable values of role are: 'READ', 'READ_WRITE', 'ADMIN'.\n" +
                        "\tProvided arguments: '%s'.\n" +
                        "\tExample of correct usage:\n" +
                        "\t\tg.call(\"%s\").with(\"username\", \"lyndon\").with(\"role\", \"ADMIN\").next();\n",
                getName(), params, getName());
    }

    @Override
    public Map<String, String> describeParams() {
        final Map<String, String> parameters = new HashMap<>();
        parameters.put("username", "The username to issue the token for.");
        parameters.put("role", "The role to issue the token for. Acceptable values are 'READ', 'READ_WRITE', 'ADMIN'.");
        return parameters;
    }

    @Override
    protected boolean sanitize(final Map params) {
        if (!params.containsKey("username") ||
                !params.containsKey("role")) {
            return false;
        }

        final String username = (String) params.get("username");
        if (username == null || username.isEmpty()) {
            return false;
        }

        final String role = (String) params.get("role");
        if (role == null || role.isEmpty() ||
                !(role.equals("READ") || role.equals("READ_WRITE") || role.equals("ADMIN"))) {
            return false;
        }

        return true;
    }

    @Override
    protected R execute(final Map params) {
        final String username = (String) params.get("username");
        final String role = (String) params.get("role");
        final JWTAuthenticator jwtAuthenticator = JWTAuthenticator.getInstance();
        if (jwtAuthenticator == null) {
            throw new IllegalStateException("Cannot issue JWT token because " +
                    "JWT authentication is not enabled on this Aerospike Graph instance.");
        }
        return (R) jwtAuthenticator.createToken(username, role);
    }

    @Override
    protected void auditLog(final Map params) {
        final String username = (String) params.get("username");
        LOGGER.info(getName() + " Creating a new JWT token for user '" + username + "'.");
    }
}
