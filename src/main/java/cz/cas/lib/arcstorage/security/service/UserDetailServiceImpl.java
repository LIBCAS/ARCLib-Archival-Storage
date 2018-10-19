package cz.cas.lib.arcstorage.security.service;

import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.security.user.UserDelegate;
import cz.cas.lib.arcstorage.security.user.UserDetails;
import cz.cas.lib.arcstorage.security.user.UserStore;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import javax.inject.Inject;

import static cz.cas.lib.arcstorage.util.Utils.notNull;

@Service
public class UserDetailServiceImpl implements UserDetailsService {
    private UserStore userStore;

    public UserDetails loadUserById(String id) {
        User user = userStore.find(id);
        notNull(user, () -> new MissingObject(User.class, id));
        return new UserDelegate(user);
    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {

        User user = userStore.findByUsername(username);
        notNull(user, () -> new MissingObject(User.class, username));

        return new UserDelegate(user);
    }

    @Inject
    public void setUserStore(UserStore store) {
        this.userStore = store;
    }
}
