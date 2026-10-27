package com.rateguard.dc.auth;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rateguard.dc.cache.ShardedLru;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;

/**
 * Validates credentials with local CPU only, so an unauthenticated request is
 * rejected before any coordinator call. Same formats as the cloud version:
 *   X-API-Key: tenant.keyId.sig   with sig = hex(HMAC-SHA256(secret, "tenant.keyId"))[:32]
 *   Authorization: Bearer <HS256 JWT>  with claims tenant, sub (key id), exp
 * Validated claims are cached in a bounded LRU (the L1 claims cache).
 */
public final class Authenticator {
    public record Claims(String tenant, String keyId, long expMillis) {}

    private final byte[] secret;
    private final ShardedLru<Claims> cache;
    private final long ttlMillis;

    public Authenticator(String secret, int cacheSize, long ttlMillis) {
        this.secret = secret.getBytes(StandardCharsets.UTF_8);
        this.cache = new ShardedLru<>(cacheSize, 16, null);
        this.ttlMillis = ttlMillis;
    }

    private byte[] hmac(String msg) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            return mac.doFinal(msg.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) { throw new IllegalStateException(e); }
    }

    private String sign(String msg) { return HexFormat.of().formatHex(hmac(msg)).substring(0, 32); }

    /** Issues an API key for (tenant, keyId). */
    public String mintKey(String tenant, String keyId) { return tenant + "." + keyId + "." + sign(tenant + "." + keyId); }

    /** Issues an HS256 JWT. */
    public String mintJwt(String tenant, String sub, long ttlSeconds) {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String h = enc.encodeToString("{\"alg\":\"HS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
        long exp = System.currentTimeMillis() / 1000 + ttlSeconds;
        String p = enc.encodeToString(("{\"tenant\":\"" + tenant + "\",\"sub\":\"" + sub + "\",\"exp\":" + exp + "}").getBytes(StandardCharsets.UTF_8));
        return h + "." + p + "." + enc.encodeToString(hmac(h + "." + p));
    }

    /** Returns the claims, or null when the credential is missing or invalid. */
    public Claims authenticate(String authorization, String apiKey, long now) {
        String cred = null;
        if (authorization != null && authorization.startsWith("Bearer ")) cred = authorization.substring(7);
        else if (apiKey != null && !apiKey.isEmpty()) cred = apiKey;
        if (cred == null) return null;

        Claims c = cache.get(cred);
        if (c != null && now < c.expMillis()) return c;

        c = (cred.chars().filter(ch -> ch == '.').count() == 2 && cred.startsWith("ey")) ? parseJwt(cred, now) : parseKey(cred);
        if (c == null) return null;
        long exp = now + ttlMillis;
        if (c.expMillis() > 0 && c.expMillis() < exp) exp = c.expMillis();
        cache.put(cred, new Claims(c.tenant(), c.keyId(), exp));
        return c;
    }

    private Claims parseKey(String k) {
        String[] p = k.split("\\.");
        if (p.length != 3 || p[0].isEmpty() || p[1].isEmpty()) return null;
        String want = sign(p[0] + "." + p[1]);
        if (!MessageDigest.isEqual(want.getBytes(StandardCharsets.UTF_8), p[2].getBytes(StandardCharsets.UTF_8))) return null;
        return new Claims(p[0], p[1], 0);
    }

    private Claims parseJwt(String tok, long now) {
        try {
            String[] p = tok.split("\\.");
            if (p.length != 3) return null;
            Base64.Decoder dec = Base64.getUrlDecoder();
            JsonObject hdr = JsonParser.parseString(new String(dec.decode(p[0]), StandardCharsets.UTF_8)).getAsJsonObject();
            if (!hdr.has("alg") || !"HS256".equals(hdr.get("alg").getAsString())) return null; // rejects "none"
            if (!MessageDigest.isEqual(dec.decode(p[2]), hmac(p[0] + "." + p[1]))) return null;
            JsonObject cl = JsonParser.parseString(new String(dec.decode(p[1]), StandardCharsets.UTF_8)).getAsJsonObject();
            if (!cl.has("tenant") || !cl.has("sub")) return null;
            long exp = cl.has("exp") ? cl.get("exp").getAsLong() * 1000 : 0;
            if (exp > 0 && now >= exp) return null;
            return new Claims(cl.get("tenant").getAsString(), cl.get("sub").getAsString(), exp);
        } catch (Exception e) {
            return null;
        }
    }
}
