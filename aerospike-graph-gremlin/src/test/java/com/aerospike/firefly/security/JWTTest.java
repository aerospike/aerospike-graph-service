package com.aerospike.firefly.security;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.JWTCreator.Builder;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.TokenExpiredException;
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import org.junit.Assert;
import org.junit.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.ECKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class JWTTest {

    public static KeyPair genKey() {
        KeyPairGenerator keyPairGenerator = null;
        try {
            keyPairGenerator = KeyPairGenerator.getInstance("EC");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        keyPairGenerator.initialize(256); // P-256 curve
        return keyPairGenerator.generateKeyPair();
    }

    @Test
    public void createCheckJWT() {
        final KeyPair keyPair = genKey();
        ECPrivateKey privateKey = (ECPrivateKey) keyPair.getPrivate();
        ECPublicKey publicKey = (ECPublicKey) keyPair.getPublic();



        // Create Algorithm instance
        Algorithm signingKeyPair = Algorithm.ECDSA256(publicKey, privateKey);


        final String j = JWT.create()
                .withClaim("role", "ADMIN")
                .withClaim("role", "READ")
                .withClaim("role", "WRITE")
                .withSubject("Grant")
                .withIssuer("Aerospike")
                .withKeyId("somekey")
                .sign(Algorithm.ECDSA256(publicKey, privateKey));
        System.out.println("Encoded JWT: " + j);
        DecodedJWT decodedJWT = JWT.decode(j);
        String subject = decodedJWT.getSubject();
        String issuer = decodedJWT.getIssuer();
        String keyId = decodedJWT.getKeyId();
        Map<String, Claim> claims = decodedJWT.getClaims();
        Assert.assertEquals("Grant", subject);
        Assert.assertEquals("Aerospike", issuer);
        Assert.assertEquals("somekey", keyId);
        Assert.assertEquals(3, claims.size());


        final JWTVerifier verifier = JWT.require(signingKeyPair)
                .withIssuer(issuer)
                .build();
        verifier.verify(j);

    }

    @Test
    public void createCheckExpiredJWT() {
        final KeyPair keyPair = genKey();
        ECPrivateKey privateKey = (ECPrivateKey) keyPair.getPrivate();
        ECPublicKey publicKey = (ECPublicKey) keyPair.getPublic();

        // Create Algorithm instance
        Algorithm signingKeyPair = Algorithm.ECDSA256(publicKey, privateKey);
        final String j = JWT.create().withClaim("role", "ADMIN")
                .withClaim("role", "READ")
                .withClaim("role", "WRITE")
                .withSubject("Grant")
                .withIssuer("Aerospike")
                .withKeyId("somekey")
                .withExpiresAt(Instant.now().minus(1, ChronoUnit.MILLIS))
                .sign(Algorithm.ECDSA256(publicKey, privateKey));
        System.out.println("Encoded JWT: " + j);
        DecodedJWT decodedJWT = JWT.decode(j);
        String subject = decodedJWT.getSubject();
        String issuer = decodedJWT.getIssuer();
        String keyId = decodedJWT.getKeyId();
        Map<String, Claim> claims = decodedJWT.getClaims();
        Assert.assertEquals("Grant", subject);
        Assert.assertEquals("Aerospike", issuer);
        Assert.assertEquals("somekey", keyId);
        Assert.assertEquals(4, claims.size());


        final JWTVerifier verifier = JWT.require(signingKeyPair)
                .withIssuer(issuer)
                .build();
        boolean passed = false;
        try {
            verifier.verify(j);
        } catch (TokenExpiredException tokenExpiredException) {
            passed = true;
        }
        assertTrue("should not validate expired token", passed);

    }
}
