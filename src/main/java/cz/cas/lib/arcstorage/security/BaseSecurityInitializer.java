package cz.cas.lib.arcstorage.security;

import cz.cas.lib.arcstorage.security.basic.BasicAuthenticationFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.Filter;

/**
 * Configurator for authorization and authentication.
 *
 * <p>
 *     Configures JWT secondary authentication and authorization.
 * </p>
 * <p>
 *     Developer should extend this class and provide {@link AuthenticationProvider} and {@link OncePerRequestFilter}
 *     for primary authentication scheme.
 * </p>
 */
@EnableGlobalMethodSecurity(jsr250Enabled = true, prePostEnabled = true)
@EnableWebSecurity
public abstract class BaseSecurityInitializer extends WebSecurityConfigurerAdapter {

    private String[] urlPatterns() {
        return new String[]{"/api/**"};
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        HttpSecurity httpSecurity = http
                .requestMatchers()
                .antMatchers(urlPatterns())
                .and()
                .csrf().disable()
                .sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS).and()
                .exceptionHandling().and()
                .headers()
                    .cacheControl().and()
                    .frameOptions().disable()
                .and()
                .authorizeRequests().anyRequest().permitAll().and();

        Filter[] filters = primarySchemeFilters();
        for (Filter filter : filters) {
            httpSecurity = httpSecurity.addFilterBefore(filter, AnonymousAuthenticationFilter.class);
        }
    }

    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        AuthenticationProvider[] providers = primaryAuthProviders();
        for (AuthenticationProvider provider : providers) {
            auth = auth.authenticationProvider(provider);
        }
    }

    @Bean
    public AuthenticationManager authenticationManagerBean() throws Exception {
        return super.authenticationManagerBean();
    }

    /**
     * Provides primary auth scheme filters.
     *
     * <p>
     *     E.g. {@link BasicAuthenticationFilter}
     * </p>
     * @return Filters
     * @throws Exception Any exception will halt starting
     */
    protected abstract Filter[] primarySchemeFilters() throws Exception;

    /**
     * Provides primary auth scheme providers.
     *
     * <p>
     *     E.g. {@link DaoAuthenticationProvider}
     * </p>
     * @return Authentication providers
     * @throws Exception Any exception will halt starting
     */
    protected abstract AuthenticationProvider[] primaryAuthProviders() throws Exception;
}
