package cz.cas.lib.arcstorage.security.user;

import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.security.Role;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class UserDelegate implements UserDetails {
    private User user;

    private Set<GrantedAuthority> authorities = new HashSet<>();

    public UserDelegate(User user) {
        this(user, null);
    }

    public UserDelegate(User user, Collection<? extends GrantedAuthority> additionalAuthorities) {
        this.user = user;
        if(user.getRole()!=null)
            authorities.add(new SimpleGrantedAuthority(user.getRole().toString()));

        if (additionalAuthorities != null) {
            authorities.addAll(additionalAuthorities);
        }
    }

    @Override
    public User getUser() {
        return user;
    }

    @Override
    public String getPassword() {
        return user.getPassword();
    }

    @Override
    public String getDataSpace() {
        return user.getDataSpace();
    }

    @Override
    public Role getRole() {
        return user.getRole();
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
        return true;
    }

    @Override
    public Collection<GrantedAuthority> getAuthorities() {
        return authorities;
    }
}
