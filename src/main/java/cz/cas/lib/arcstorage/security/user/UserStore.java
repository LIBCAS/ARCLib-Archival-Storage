package cz.cas.lib.arcstorage.security.user;

import cz.cas.lib.arcstorage.domain.entity.QUser;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.domain.store.DatedStore;
import org.springframework.stereotype.Repository;

@Repository
public class UserStore extends DatedStore<User, QUser> {
    public UserStore() {
        super(User.class, QUser.class);
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
}
