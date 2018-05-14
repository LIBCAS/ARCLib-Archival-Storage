package cz.cas.lib.arcstorage.security.basic;

import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.crypto.codec.Base64;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

public class BasicAuthenticationFilter
        extends org.springframework.security.web.authentication.www.BasicAuthenticationFilter {

    public BasicAuthenticationFilter(AuthenticationManager authenticationManager) {
        super(authenticationManager);
    }

    private String extractUsername(HttpServletRequest request) throws IOException {
        String header = request.getHeader("Authorization");

        if (header == null || !header.startsWith("Basic ")) {
            return null;
        }

        String[] tokens = extractAndDecodeHeader(header, request);
        assert tokens.length == 2;

        return tokens[0];
    }

    /**
     * Decodes the header into a username and password.
     *
     * @throws BadCredentialsException if the Basic header is not present or is not valid
     * Base64
     */
    private String[] extractAndDecodeHeader(String header, HttpServletRequest request)
            throws IOException {

        byte[] base64Token = header.substring(6).getBytes("UTF-8");
        byte[] decoded;
        try {
            decoded = Base64.decode(base64Token);
        }
        catch (IllegalArgumentException e) {
            throw new BadCredentialsException(
                    "Failed to decode basic authentication token");
        }

        String token = new String(decoded, getCredentialsCharset(request));

        int delim = token.indexOf(":");

        if (delim == -1) {
            throw new BadCredentialsException("Invalid basic authentication token");
        }
        return new String[] { token.substring(0, delim), token.substring(delim + 1) };
    }
}
