package cz.cas.lib.arcstorage.security.jwt;

import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.security.user.UserStore;
import cz.cas.lib.arcstorage.security.user.UserDelegate;
import cz.cas.lib.arcstorage.security.user.UserDetails;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

@Service
public class ArchivalStorageJwtHandler implements JwtHandler {
    private UserStore store;

    @Override
    public UserDetails parseClaims(Map<String, Object> claims) {
        String userId = (String) claims.get("sub");
        @SuppressWarnings("unchecked") ArrayList<String> authorityNames = (ArrayList<String>) claims.get("aut");

        Set<GrantedAuthority> authorities = null;
        if (authorityNames != null) {
            authorities = authorityNames.stream()
                    .map(SimpleGrantedAuthority::new)
                    .collect(Collectors.toSet());
        }

        User user = store.find(userId);
        if (user != null) {
            return new UserDelegate(user, true, authorities);
        }

        return null;
    }

    @Override
    public Map<String, Object> createClaims(UserDetails userDetails) {
        if (userDetails instanceof UserDelegate) {
            UserDelegate delegate = (UserDelegate) userDetails;
            User user = delegate.getUser();

            Map<String, Object> claims = new LinkedHashMap<>();
            claims.put("sub", user.getId());
            claims.put("name", user.getUsername());


            Collection<GrantedAuthority> authorities = delegate.getAuthorities();
            if (authorities != null) {
                String[] authorityNames = authorities.stream()
                        .map(GrantedAuthority::getAuthority)
                        .toArray(String[]::new);
                claims.put("aut", authorityNames);
            }

            return claims;
        } else {
            return emptyMap();
        }
    }

    @Inject
    public void setStore(UserStore store) {
        this.store = store;
    }
}
