package helper;

import com.querydsl.jpa.impl.JPAQueryFactory;
import cz.cas.lib.arcstorage.domain.store.DomainStore;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import lombok.Getter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.hibernate.internal.SessionImpl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.sql.SQLException;
import java.sql.Statement;

public abstract class DbTest {
    @Getter
    private static EntityManagerFactory factory;

    private EntityManager em;

    protected EntityManager getEm() {
        return em;
    }

    protected void flushCache() {
        if (em != null) {
            em.getTransaction().commit();
        } else {
            em = factory.createEntityManager();
        }

        em.getTransaction().begin();
    }

    @BeforeClass
    public static void classSetUp() throws Exception {
        Logger.getRootLogger().setLevel(Level.INFO);

        factory = Persistence.createEntityManagerFactory("test");
    }

    @AfterClass
    public static void classTearDown() throws Exception {
        if (factory != null) {
            factory.close();
            factory = null;
        }
    }

    @Before
    public void testSetUp() throws Exception {
        flushCache();

        setSyntax();
    }

    @After
    public void testTearDown() throws Exception {
        if (em != null) {
            clearDatabase();

            em.close();
            em = null;
        }
    }

    public void setSyntax() throws SQLException {
        ((SessionImpl) em.getDelegate()).doWork(c -> {
                    Statement s = c.createStatement();
                    s.execute("SET DATABASE SQL SYNTAX PGS TRUE");
                    s.close();
                }
        );
    }

    public void clearDatabase() throws SQLException {
        ((SessionImpl) em.getDelegate()).doWork(c -> {
                    Statement s = c.createStatement();

                    //s.execute("DROP SCHEMA PUBLIC CASCADE ");
                    s.execute("TRUNCATE SCHEMA PUBLIC AND COMMIT");
                    s.close();
                }
        );
    }

    public void initializeStores(DomainStore... stores) {
        for (DomainStore store : stores) {
            store.setEntityManager(em);
            store.setQueryFactory(new JPAQueryFactory(em));
        }
    }
}
