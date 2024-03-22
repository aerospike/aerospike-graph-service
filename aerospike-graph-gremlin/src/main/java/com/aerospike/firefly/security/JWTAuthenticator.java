package com.aerospike.firefly.security;

import com.aerospike.firefly.runtime.FireflyServer;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.impl.JWTParser;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.Header;
import com.auth0.jwt.interfaces.JWTVerifier;
import com.auth0.jwt.interfaces.Payload;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;

public class JWTAuthenticator implements Authenticator {
    private static final Logger logger = LoggerFactory.getLogger(FireflyServer.class);
    private static final byte NUL = 0;

    private String secret;
    private String issuer;
    private final Algorithm algorithm;
    private final JWTVerifier verifier;

    public JWTAuthenticator(String subject, long tokenValidityInMillis) {

        this.algorithm = Algorithm.HMAC256(secret);
        this.verifier = JWT.require(algorithm)
                .withIssuer(issuer)
                .build();
    }

    @Override
    public boolean requireAuthentication() {
        return true;
    }

    @Override
    public void setup(final Map<String, Object> config) {
        logger.info("Initializing authentication with the {}", SimpleAuthenticator.class.getName());

        if (null == config || config.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                    "Could not configure a %s - provide a 'config' in the 'authentication' settings",
                    SimpleAuthenticator.class.getName()));
        }
        Configuration configuration = new MapConfiguration(config);

        if (!config.containsKey(ConfigurationHelper.Keys.JWT_SECRET)) {
            throw new IllegalStateException(String.format(
                    "Configuration missing the %s key", ConfigurationHelper.Keys.JWT_SECRET));
        }
        secret = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.JWT_SECRET, configuration);
        if (!config.containsKey(ConfigurationHelper.Keys.JWT_ISSUER)) {
            throw new IllegalStateException(String.format(
                    "Configuration missing the %s key", ConfigurationHelper.Keys.JWT_ISSUER));
        }
        issuer = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.JWT_ISSUER, configuration);

    }

    @Override
    public SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress) {
        return new PlainTextSaslAuthenticator();
    }

    public DecodedJWT verifyJWT(String jwtToken) {
        return verifier.verify(jwtToken);
    }

    private DecodedJWT decodeJWT(String jwtToken) {
        return JWT.decode(jwtToken);
    }

    @Override
    public AuthenticatedUser authenticate(final Map<String, String> credentials) throws AuthenticationException {
        if (!credentials.containsKey(PROPERTY_USERNAME))
            throw new IllegalArgumentException(String.format("Credentials must contain a %s", PROPERTY_USERNAME));
        if (!credentials.containsKey(PROPERTY_PASSWORD))
            throw new IllegalArgumentException(String.format("Credentials must contain a %s", PROPERTY_PASSWORD));
        final DecodedJWT jwt;
        try {
            jwt = verifyJWT(credentials.get(PROPERTY_PASSWORD));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Error: %s Could not authenticate jwt %s : ", e.getMessage(), credentials.get(PROPERTY_PASSWORD)));
        }
        JWTParser parser = new JWTParser();
        final Header header = parser.parseHeader(jwt.getHeader());
        final Payload payload = parser.parsePayload(jwt.getPayload());
        try {
            assert payload.getSubject().equals(credentials.get(PROPERTY_USERNAME));
        } catch (AssertionError assertionError) {
            throw new RuntimeException(String.format("Username %s does not equal token subject %s", credentials.get(PROPERTY_USERNAME), payload.getSubject()));
        }
        try {
            assert payload.getExpiresAtAsInstant().isAfter(Instant.now());
        } catch (AssertionError e) {
            throw new RuntimeException(String.format("JWT expired: %s", payload.getExpiresAt()));
        }


        return new JWTAuthenticatedUser(payload);
    }

    protected class JWTAuthenticatedUser extends AuthenticatedUser implements UserContext{

        private final Payload jwtPayload;

        public JWTAuthenticatedUser(final Payload payload) {
            super(payload.getSubject());
            this.jwtPayload = payload;
        }

        @Override
        public List<ROLE> getRoles() {
            return jwtPayload
                    .getClaim("roles")
                    .asList(String.class)
                    .stream()
                    .map(ROLE::valueOf)
                    .collect(Collectors.toList());
        }

        @Override
        public boolean valid(FireflyGraph fireflyGraph) {
            return jwtPayload.getExpiresAtAsInstant().isAfter(Instant.now()) && fireflyGraph.getBaseGraph().userIsValid(this);
        }
    }
    private class PlainTextSaslAuthenticator implements Authenticator.SaslNegotiator {
        private boolean complete = false;
        private String username;
        private String password;

        @Override
        public byte[] evaluateResponse(final byte[] clientResponse) throws AuthenticationException {
            decodeCredentials(clientResponse);
            complete = true;
            return null;
        }

        @Override
        public boolean isComplete() {
            return complete;
        }

        @Override
        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException {
            if (!complete) throw new AuthenticationException("SASL negotiation not complete");
            final Map<String, String> credentials = new HashMap<>();
            credentials.put(PROPERTY_USERNAME, username);
            credentials.put(PROPERTY_PASSWORD, password);
            return authenticate(credentials);
        }

        /**
         * SASL PLAIN mechanism specifies that credentials are encoded in a
         * sequence of UTF-8 bytes, delimited by 0 (US-ASCII NUL).
         * The form is : {code}authzId<NUL>authnId<NUL>password<NUL>{code}.
         *
         * @param bytes encoded credentials string sent by the client
         */
        private void decodeCredentials(byte[] bytes) throws AuthenticationException {
            byte[] user = null;
            byte[] pass = null;
            int end = bytes.length;
            for (int i = bytes.length - 1; i >= 0; i--) {
                if (bytes[i] == NUL) {
                    if (pass == null)
                        pass = Arrays.copyOfRange(bytes, i + 1, end);
                    else if (user == null)
                        user = Arrays.copyOfRange(bytes, i + 1, end);
                    end = i;
                }
            }

            if (null == user) throw new AuthenticationException("Authentication ID must not be null");
            if (null == pass) throw new AuthenticationException("Password must not be null");

            username = new String(user, StandardCharsets.UTF_8);
            password = new String(pass, StandardCharsets.UTF_8);
        }
    }
}
