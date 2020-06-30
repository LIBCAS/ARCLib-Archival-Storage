package cz.cas.lib.arcstorage.util;

import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.service.ArchivalDbService;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

@Service
public class ApplicationContextUtils implements ApplicationContextAware {

    private static ApplicationContext ctx;
    /**
     * Contains currently processing objects (those in states {@link ObjectState#PRE_PROCESSING}, {@link ObjectState#PROCESSING}).
     * Objects are added during their registration ({@link ArchivalDbService#registerAipCreation(String, Checksum, Checksum)}, {@link ArchivalDbService#registerXmlUpdate(String, Checksum, Integer)}).
     * Object is removed from map in {@link ArchivalDbService#setObjectsState(ObjectState, String...)} when changing the state to some non-processing state.
     * <p>
     * The lock is used to ensure that rollback request called upon processing object is properly solved. If one thread is just
     * switching the state to {@link ObjectState#ARCHIVED}, the rollback request has to wait until it obtains lock and then decide how to continue.
     * If on the other hand the rollback request obtains lock first, it sets rollback flag and the storing thread must recognize that the flag is set
     * before it switches the state to {@link ObjectState#ARCHIVED}.
     * </p>
     */
    @Getter
    private static ConcurrentMap<String, Pair<AtomicBoolean, Lock>> processingObjects = new ConcurrentHashMap<>();


    @Override
    public void setApplicationContext(ApplicationContext context) throws BeansException {
        ctx = context;
    }

    public static ApplicationContext getApplicationContext() {
        return ctx;
    }
}
