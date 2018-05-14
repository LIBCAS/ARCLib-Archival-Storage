package cz.cas.lib.arcstorage.security.basic;

import org.springframework.security.authentication.AuthenticationManager;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Set;

import static cz.cas.lib.arcstorage.util.Utils.asSet;

public class PathBasicAuthFilter extends BasicAuthenticationFilter {
    private Set<String> authQueries;

    public PathBasicAuthFilter(AuthenticationManager authenticationManager, String authQuery) {
        super(authenticationManager);
        this.authQueries = asSet(authQuery);
    }

    public PathBasicAuthFilter(AuthenticationManager authenticationManager, String[] authQueries) {
        super(authenticationManager);
        this.authQueries = asSet(authQueries);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws ServletException, IOException {
        String query = request.getRequestURI();

        if (!authQueries.contains(query)) {
            chain.doFilter(request, response);
            return;
        }

        super.doFilterInternal(request, response, chain);
    }
}
