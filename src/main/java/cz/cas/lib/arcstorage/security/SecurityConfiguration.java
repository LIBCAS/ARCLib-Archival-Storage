package cz.cas.lib.arcstorage.security;

import cz.cas.lib.arcstorage.security.basic.BasicAuthenticationFilter;
import cz.cas.lib.arcstorage.security.service.UserDetailServiceImpl;
import cz.cas.lib.arcstorage.util.Utils;
import jakarta.servlet.Filter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.annotation.web.configurers.HeadersConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;

@Configuration
@EnableMethodSecurity(jsr250Enabled = true)
@EnableWebSecurity
public class SecurityConfiguration {

    private AuthenticationManager authenticationManager;
    private UserDetailServiceImpl userDetailsService;
    private PasswordEncoder encoder;

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {

        DaoAuthenticationProvider authenticationProvider = new DaoAuthenticationProvider();
        authenticationProvider.setPasswordEncoder(encoder);
        authenticationProvider.setUserDetailsService(userDetailsService);

        HttpSecurity httpSecurity = http
                .authenticationProvider(authenticationProvider)
                .securityMatchers(c -> c.requestMatchers(urlPatterns()))
                .sessionManagement(c -> c.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .headers(c -> {
                    c.cacheControl(Customizer.withDefaults());
                    c.frameOptions(HeadersConfigurer.FrameOptionsConfig::disable);
                })
                .authorizeHttpRequests(c -> c.anyRequest().permitAll())
                .csrf(AbstractHttpConfigurer::disable);

        Filter[] filters = primarySchemeFilters();
        for (Filter filter : filters) {
            httpSecurity = httpSecurity.addFilterBefore(filter, AnonymousAuthenticationFilter.class);
        }

        return http.build();
    }

    private String[] urlPatterns() {
        return new String[]{"/api/**"};
    }

    private Filter[] primarySchemeFilters() throws Exception {
        BasicAuthenticationFilter basicFilter = new BasicAuthenticationFilter(authenticationManager);
        return Utils.asArray(basicFilter);
    }

    @Autowired
    @Lazy
    public void setAuthenticationManager(AuthenticationManager authenticationManager) {
        this.authenticationManager = authenticationManager;
    }

    @Autowired
    public void setUserDetailsService(UserDetailServiceImpl userDetailsService) {
        this.userDetailsService = userDetailsService;
    }

    @Autowired
    public void setEncoder(PasswordEncoder encoder) {
        this.encoder = encoder;
    }
}
