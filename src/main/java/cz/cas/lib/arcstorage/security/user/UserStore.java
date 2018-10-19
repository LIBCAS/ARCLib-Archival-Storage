package cz.cas.lib.arcstorage.security.user;

import cz.cas.lib.arcstorage.domain.entity.QUser;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.domain.store.DatedStore;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.security.Role;
import org.springframework.stereotype.Repository;

import java.util.List;

import static cz.cas.lib.arcstorage.util.Utils.notNull;

@Repository
public class UserStore extends DatedStore<User, QUser> {
    public UserStore() {
        super(User.class, QUser.class);
    }

    @Transactional
    public User save(User entity) {
        notNull(entity, () -> new IllegalArgumentException("entity"));

        User obj = entityManager.merge(entity);

        entityManager.flush();
        detachAll();

        return obj;
    }

    public User findByUsername(String username) {
        QUser qUser = qObject();

        User user = query()
                .select(qUser)
                .where(qUser.username.eq(username))
                .where(qUser.deleted.isNull())
                .where(findWhereExpression())
                .fetchFirst();

        detachAll();
        return user;
    }

    public List<User> findByRole(Role role) {
        List<User> fetch = query().select(qObject()).where(qObject().role.eq(role)).fetch();
        detachAll();
        return fetch;
    }
}
