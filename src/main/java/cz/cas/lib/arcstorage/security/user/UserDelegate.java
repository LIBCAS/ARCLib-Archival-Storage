package cz.cas.lib.arcstorage.security.user;

import cz.cas.lib.arcstorage.domain.entity.User;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class UserDelegate implements UserDetails {
    private User user;

    private Set<GrantedAuthority> authorities = new HashSet<>();

    private boolean enabled;

    public UserDelegate(User user, Boolean enabled) {
        this(user, enabled, null);
    }

    public UserDelegate(User user, Boolean enabled, Collection<? extends GrantedAuthority> additionalAuthorities) {
        this.user = user;

            authorities.add(new SimpleGrantedAuthority("ROLE_USER"));


        if (additionalAuthorities != null) {
            authorities.addAll(additionalAuthorities);
        }

        if (enabled != null) {
            this.enabled = enabled;
        }
    }

    public User getUser() {
        return user;
    }

    @Override
    public String getPassword() {
        return user.getPassword();
    }

    @Override
    public String getUsername() {
        if (user != null) {
            return user.getUsername();
        } else {
            return null;
        }
    }

    @Override
    public String getId() {
        if (user != null) {
            return user.getId();
        } else {
            return null;
        }
    }

    @Override
    public boolean isAccountNonExpired() {
        return true;
    }

    @Override
    public boolean isAccountNonLocked() {
        return true;
    }

    @Override
    public boolean isCredentialsNonExpired() {
        return true;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public Collection<GrantedAuthority> getAuthorities() {
        return authorities;
    }
}
