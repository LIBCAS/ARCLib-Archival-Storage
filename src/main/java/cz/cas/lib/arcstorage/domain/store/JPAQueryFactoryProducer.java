package cz.cas.lib.arcstorage.domain.store;

import com.querydsl.jpa.impl.JPAQueryFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import jakarta.persistence.EntityManager;

@Configuration
public class JPAQueryFactoryProducer {
    /**
     * Produces QueryDSL {@link JPAQueryFactory} used in {@link DomainStore}.
     *
     * @param entityManager Provided JPA {@link EntityManager}
     * @return produced {@link JPAQueryFactory}
     */
    @Bean
    public JPAQueryFactory jpaQueryFactory(EntityManager entityManager) {
        return new JPAQueryFactory(entityManager);
    }
}
