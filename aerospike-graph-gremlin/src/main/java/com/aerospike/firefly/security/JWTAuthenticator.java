package com.aerospike.firefly.security;

import com.aerospike.firefly.util.ConfigurationHelper;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.JWTVerifier;
import java.util.Base64;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;

public class JWTAuthenticator implements Authenticator {
    private static final Logger LOG = LoggerFactory.getLogger(JWTAuthenticator.class);
    // Other algorithms require inputs that aren't just secret. We could support them later but for now doing minimum.
    public static final Set<String> SUPPORTED_ALGORITHMS = Set.of("HMAC256", "HMAC384", "HMAC512");

    private static final byte NUL = 0;
    // Do not store secret and issuer for security reasons, just store the verifier.
    private JWTVerifier verifier;
    private static JWTAuthenticator INSTANCE = null;
    private String issuer = null;
    private Algorithm algo = null;

    public JWTAuthenticator() {
    }

    @Override
    public boolean requireAuthentication() {
        return true;
    }

    public String createToken(final String username, final String role) {
        if (algo == null || issuer == null) {
            // Should never happen since we got an instance.
            throw new IllegalStateException("Cannot issue JWT token; JWTAuthenticator is not initialized.");
        }
        return JWT.create()
                .withSubject(username)
                .withClaim("role", role)
                .withIssuer(issuer)
                .sign(algo);
    }

    public String createToken(final String username, final String role, final Number expiry) {
        if (algo == null || issuer == null) {
            // Should never happen since we got an instance.
            throw new IllegalStateException("Cannot issue JWT token; JWTAuthenticator is not initialized.");
        }
        return JWT.create()
                .withSubject(username)
                .withClaim("role", role)
                .withIssuer(issuer)
                .withExpiresAt(Instant.now().plusSeconds(expiry.longValue()))
                .sign(algo);
    }

    public static JWTAuthenticator getInstance() {
        if (INSTANCE == null) {
            // Should never happen.
            // Full name b/c of the ambiguity with the other AuthenticationException.
            throw com.aerospike.firefly.util.exceptions.AuthenticationException.authNotInitialized();
        }
        return INSTANCE;
    }

    @Override
    public void setup(final Map<String, Object> config) {
        LOG.info("Initializing authentication with the {}", SimpleAuthenticator.class.getName());

        if (null == config || config.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                    "Could not configure a %s - provide a 'config' in the 'authentication' settings",
                    SimpleAuthenticator.class.getName()));
        }
        final Configuration configuration = new MapConfiguration(config);
        final List<String> missingKeys = new ArrayList<>();
        if (!config.containsKey(ConfigurationHelper.Keys.JWT_SECRET)) {
            missingKeys.add(ConfigurationHelper.Keys.JWT_SECRET);
        }
        if (!config.containsKey(ConfigurationHelper.Keys.JWT_ISSUER)) {
            missingKeys.add(ConfigurationHelper.Keys.JWT_ISSUER);
        }
        if (!missingKeys.isEmpty()) {
            throw new IllegalStateException(String.format("Configuration missing the following key(s) %s", missingKeys));
        }
        if (config.containsKey(ConfigurationHelper.Keys.JWT_ALGORITHM)) {
            final String algorithm = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.JWT_ALGORITHM, configuration);
            if (algorithm == null) {
                throw new IllegalArgumentException(ConfigurationHelper.Keys.JWT_ALGORITHM + " cannot be null, must be one of " + SUPPORTED_ALGORITHMS + ".");
            }
            final String secret = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.JWT_SECRET, configuration);
            if (secret == null) {
                throw new IllegalArgumentException(ConfigurationHelper.Keys.JWT_SECRET + " cannot be null.");

            }
            switch (algorithm.toUpperCase()) {
                case "HMAC256":
                    algo = Algorithm.HMAC256(secret);
                    break;
                case "HMAC384":
                    algo = Algorithm.HMAC384(secret);
                    break;
                case "HMAC512":
                    algo = Algorithm.HMAC512(secret);
                    break;
                default:
                    throw new IllegalArgumentException(String.format(
                            ConfigurationHelper.Keys.JWT_ALGORITHM + " '%s' is not supported, supported algorithms are %s.", algorithm, SUPPORTED_ALGORITHMS));
            }
        }
        issuer = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.JWT_ISSUER, configuration);
        verifier = JWT.require(algo).
                withIssuer(issuer)
                .build();
        INSTANCE = this;
    }

    @Override
    public SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress) {
        return new PlainTextSaslAuthenticator();
    }

    public AuthenticatedUser authenticate(final String token) throws AuthenticationException{
        final DecodedJWT jwt;
        try {
            jwt = verifier.verify(new String(Base64.getMimeDecoder().decode(token)));
        } catch (final Exception e) {
            throw new AuthenticationException(String.format("Failure to validate credentials: %s", e.getMessage()));
        }
        final Instant expiry = jwt.getExpiresAtAsInstant();
        if (expiry != null && expiry.isBefore(Instant.now())) {
            throw new AuthenticationException(String.format("JWT is already expired, expiry date: %s", jwt.getExpiresAtAsInstant()));
        }
        return new JWTAuthenticatedUser(jwt);
    }

    @Override
    public AuthenticatedUser authenticate(final Map<String, String> credentials) throws AuthenticationException {
        if (!credentials.containsKey(PROPERTY_USERNAME))
            throw new AuthenticationException(String.format("Credentials must contain a %s", PROPERTY_USERNAME));
        if (!credentials.containsKey(PROPERTY_PASSWORD))
            throw new AuthenticationException(String.format("Credentials must contain a %s", PROPERTY_PASSWORD));

        final DecodedJWT jwt;
        try {
            jwt = verifier.verify(credentials.get(PROPERTY_PASSWORD));
        } catch (final Exception e) {
            throw new AuthenticationException(String.format("Failure to validate credentials: %s", e.getMessage()));
        }

        final Instant expiry = jwt.getExpiresAtAsInstant();
        if (expiry != null && expiry.isBefore(Instant.now())) {
            throw new AuthenticationException(String.format("JWT is already expired, expiry date: %s", jwt.getExpiresAtAsInstant()));
        }

        final String subject = jwt.getSubject();
        if (subject == null || !jwt.getSubject().equals(credentials.get(PROPERTY_USERNAME))) {
            // Should we return the values back? Maybe we should just throw an exception without info.
            throw new AuthenticationException("User does not match token subject");
        }

        return new JWTAuthenticatedUser(jwt);
    }

    public static class JWTAuthenticatedUser extends AuthenticatedUser implements UserContext {

        private final DecodedJWT decodedJWT;

        public JWTAuthenticatedUser(final DecodedJWT decodedJWT) {
            super(decodedJWT.getSubject());
            this.decodedJWT = decodedJWT;
        }

        @Override
        public ROLE getRole() {
            // remove '"' from each side
            try {
                if (decodedJWT.getClaims().get("role") == null) {
                    return null;
                }
                return ROLE.valueOf(decodedJWT.getClaims().get("role").toString().replaceAll("\"", ""));
            } catch (final IllegalArgumentException e) {
                // This is used to bubble up appropriate exceptions.
                return null;
            }
        }

        @Override
        public boolean valid() {
            final Instant expiry = decodedJWT.getExpiresAtAsInstant();
            return expiry == null || expiry.isAfter(Instant.now());
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
