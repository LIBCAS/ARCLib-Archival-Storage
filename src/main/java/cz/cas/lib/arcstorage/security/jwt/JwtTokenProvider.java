package cz.cas.lib.arcstorage.security.jwt;

import cz.cas.lib.arcstorage.security.user.UserDetails;
import io.jsonwebtoken.ClaimJwtException;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

/**
 * Spring {@link AuthenticationProvider} implementation based on JWT tokens.
 *
 * <p>
 *     Encapsulates parsing, validation and generating of {@link JwtToken}.
 * </p>
 */
@Service
public class JwtTokenProvider implements AuthenticationProvider {
    private String secret;

    private Long expiration;

    private JwtHandler handler;

    @Override
    public JwtToken authenticate(Authentication authentication) throws AuthenticationException {

        JwtToken token = (JwtToken) authentication;

        if (token.getPrincipal() instanceof String) {

            try {
                Claims claims = Jwts.parser()
                        .setSigningKey(secret)
                        .parseClaimsJws((String) token.getPrincipal())
                        .getBody();

                UserDetails user = handler.parseClaims(claims);

                return new JwtToken(user, claims, user.getAuthorities());
            } catch (ClaimJwtException ex) {
                throw new BadCredentialsException("JWT error", ex);
            }
        } else {
            return null;
        }
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return JwtToken.class.isAssignableFrom(authentication);
    }

    public String generateToken(UserDetails user) {

        Map<String, Object> claims = handler.createClaims(user);

        Instant now = Instant.now();
        return Jwts.builder()
                .setClaims(claims)
                .setIssuedAt(Date.from(now))
                .setExpiration(Date.from(now.plusSeconds(expiration)))
                .signWith(SignatureAlgorithm.HS256, secret)
                .compact();

    }

    /**
     * Sets the secret used for token signing.
     *
     * @param secret Provided secret
     */
    @Inject
    public void setSecret(@Value("${security.jwt.secret}") String secret) {
        this.secret = secret;
    }

    /**
     * Sets the token validity duration
     * @param expiration Provided validity in seconds
     */
    @Inject
    public void setExpiration(@Value("${security.jwt.expiration:300}") Long expiration) {
        this.expiration = expiration;
    }

    @Inject
    public void setHandler(JwtHandler handler) {
        this.handler = handler;
    }
}
