package it.pagopa.cruscotto.ingestion.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.util.Base64;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.*;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;

public class PreDecodeJwtDecoder implements JwtDecoder {

    private final JwtDecoder delegate;
    private final OAuth2TokenValidator<Jwt> validator = JwtValidators.createDefault();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // se true, ignora la verifica della firma per token non firmati
    private boolean skipAuthenticationVerify = false;
    private boolean skipSignVerify = true;


    public PreDecodeJwtDecoder(JwtDecoder delegate) {
        this.delegate = delegate;
    }

    @Override
    public Jwt decode(String token) throws JwtException {
            // prova a parsare il token come JWS
            if (skipSignVerify) {
                return parseUnsignedToken(token);

            } else {
                return delegate.decode(token);

            }


    }

    private Jwt parseUnsignedToken(String token) {
        Map<String, Object> claims = extractClaims(token);
        if (claims.get("exp") != null) {
            long exp = ((Number) claims.get("exp")).longValue();
            claims.put("exp", Instant.ofEpochSecond(exp));
        }
        if (claims.get("iat") != null) {
            long iat = ((Number) claims.get("iat")).longValue();
            claims.put("iat", Instant.ofEpochSecond(iat));
        }

        Jwt jwt = Jwt.withTokenValue(token)
            .headers(h -> h.put("alg", "none"))
            .claims(c -> c.putAll(claims))
            .build();

        if (!skipAuthenticationVerify) {
            validateJwt(jwt);
        }

        return jwt;
    }

    private Jwt validateJwt(Jwt jwt) {
        OAuth2TokenValidatorResult result = validator.validate(jwt);
        if (result.hasErrors()) {
            Collection<OAuth2Error> errors = result.getErrors();
            throw new JwtValidationException("", errors);
        }
        return jwt;
    }

    public static Map<String, Object> extractClaims(String token) {
        try {
            String[] parts = token.split("\\.");
            if (parts.length < 2) throw new IllegalArgumentException("Token JWT non valido");

            byte[] decoded = Base64.from(parts[1]).decode();
            return objectMapper.readValue(decoded, Map.class);
        } catch (Exception e) {
            throw new RuntimeException("Errore durante l'estrazione delle claims dal JWT", e);
        }
    }
}
