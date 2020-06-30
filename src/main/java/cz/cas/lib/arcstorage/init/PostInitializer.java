package cz.cas.lib.arcstorage.init;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import com.fasterxml.jackson.datatype.hibernate5.Hibernate5Module;
import cz.cas.lib.arcstorage.domain.entity.DomainObject;
import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.domain.store.ArchivalObjectStore;
import cz.cas.lib.arcstorage.mail.ArcstorageMailCenter;
import cz.cas.lib.arcstorage.service.IntervalJobService;
import cz.cas.lib.arcstorage.service.StorageProvider;
import cz.cas.lib.arcstorage.service.SystemAdministrationService;
import cz.cas.lib.arcstorage.service.SystemStateService;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storagesync.exception.SynchronizationInProgressException;
import cz.cas.lib.arcstorage.util.ApplicationContextUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.util.Utils.resourceString;

@Component
@Slf4j
public class PostInitializer implements ApplicationListener<ApplicationReadyEvent> {
    @Inject
    private DataSource ds;
    @Inject
    private ObjectMapper objectMapper;
    @Inject
    private ArcstorageMailCenter arcstorageMailCenter;
    @Inject
    private IntervalJobService intervalJobService;

    @Value("${env}")
    private String env;

    @Value("${arcstorage.cleanUpAtApplicationStart:false}")
    private boolean startUpCleanUp;
    @Inject
    private StorageProvider storageProvider;
    @Inject
    private SystemStateService systemStateService;
    @Inject
    private SystemAdministrationService systemAdministrationService;
    @Inject
    private ArchivalObjectStore archivalObjectStore;

    @Override
    public void onApplicationEvent(ApplicationReadyEvent events) {
        objectMapper.registerModule(new Hibernate5Module());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.setDateFormat(new ISO8601DateFormat());
        SystemState systemState = systemStateService.createDefaultIfNotExist();
        if (env.equals("staging")) {
            try {
                sqlTestInit();
            } catch (Exception e) {
                throw new RuntimeException("Data init error", e);
            }
        }
        if (!env.equals("test")) {
            checkAttachedStorages(systemState);
            intervalJobService.scheduleReachabilityChecks(systemState.getReachabilityCheckIntervalInMinutes());
        }
        if (startUpCleanUp) {
            try {
                systemAdministrationService.cleanup(true);
            } catch (SomeLogicalStoragesNotReachableException | NoLogicalStorageAttachedException | IOException | SynchronizationInProgressException e) {
                throw new RuntimeException("Cant perform startup cleanup", e);
            }
        } else {
            Map<String, Pair<AtomicBoolean, Lock>> collect = archivalObjectStore.findProcessingObjects()
                    .stream()
                    .collect(Collectors.toMap(DomainObject::getId, o -> Pair.of(new AtomicBoolean(true), new ReentrantLock())));
            ApplicationContextUtils.getProcessingObjects().putAll(collect);
        }

    }

    public void sqlTestInit() throws SQLException, IOException {
        try (Connection con = ds.getConnection()) {
            ScriptRunner runner = new ScriptRunner(con, false, true);

            String initSql = resourceString("init.sql");

            String arclibXml1Sha512 = new String(Files.readAllBytes(Paths.get("data/arclib/4b/66/65/4b66655a-819a-474f-8203-6c432815df1f_xml_1.SHA512")));
            String arclibXml2Sha512 = new String(Files.readAllBytes(Paths.get("data/arclib/4b/66/65/4b66655a-819a-474f-8203-6c432815df1f_xml_2.SHA512")));
            String arclibXml3Sha512 = new String(Files.readAllBytes(Paths.get("data/arclib/8b/2e/fa/8b2efafd-b637-4b97-a8f7-1b97dd4ee622_xml_1.SHA512")));
            String arclibXml4Sha512 = new String(Files.readAllBytes(Paths.get("data/arclib/8b/2e/fa/8b2efafd-b637-4b97-a8f7-1b97dd4ee622_xml_2.SHA512")));
            String arclibXml5Sha512 = new String(Files.readAllBytes(Paths.get("data/arclib/89/f8/2d/89f82da0-af78-4461-bf92-7382050082a1_xml_1.SHA512")));

            initSql = replaceArclibXmlHash(arclibXml1Sha512, initSql, "'11f82da0-af78-4461-bf92-7382050082a1',\r\n" +
                    "        '4b66655a-819a-474f-8203-6c432815df1f',\r\n" +
                    "        '2018-03-08 07:00:00',\r\n");
            initSql = replaceArclibXmlHash(arclibXml2Sha512, initSql, "'12f82da0-af78-4461-bf92-7382050082a1',\r\n" +
                    "        '4b66655a-819a-474f-8203-6c432815df1f',\r\n" +
                    "        '2018-03-08 08:00:00',\r\n");
            initSql = replaceArclibXmlHash(arclibXml3Sha512, initSql, "'21f82da0-af78-4461-bf92-7382050082a1',\r\n" +
                    "        '8b2efafd-b637-4b97-a8f7-1b97dd4ee622',\r\n" +
                    "        '2018-03-08 07:00:00',\r\n");
            initSql = replaceArclibXmlHash(arclibXml4Sha512, initSql, "'22f82da0-af78-4461-bf92-7382050082a1',\r\n" +
                    "        '8b2efafd-b637-4b97-a8f7-1b97dd4ee622',\r\n" +
                    "        '2018-03-08 08:00:00',\r\n");
            initSql = replaceArclibXmlHash(arclibXml5Sha512, initSql, "'3182da0-af78-4461-bf92-7382050082a1',\r\n" +
                    "        '89f82da0-af78-4461-bf92-7382050082a1',\r\n" +
                    "        '2018-03-08 08:00:00',\r\n");

            runner.runScript(new StringReader(initSql));
        }
        log.info("Data init successful");
    }

    private String replaceArclibXmlHash(String hashValue, String initSql, String stringToMatch) {
        return initSql.replaceFirst(stringToMatch + "        '([a-z0-9]*)'",
                stringToMatch + "        '" + hashValue + "'");
    }

    private void checkAttachedStorages(SystemState systemState) {
        long storagesCount = storageProvider.getStoragesCount();
        int minStorageCount = systemState.getMinStorageCount();
        Pair<List<StorageService>, List<StorageService>> services = storageProvider.checkReachabilityOfAllStorages();
        if (!services.getRight().isEmpty() || storagesCount < minStorageCount)
            arcstorageMailCenter.sendInitialStoragesCheckWarning(storagesCount, minStorageCount, services.getRight());
    }
}

