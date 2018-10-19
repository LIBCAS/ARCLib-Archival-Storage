package cz.cas.lib.arcstorage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.store.ConfigurationStore;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.service.exception.ConfigParserException;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.ceph.CephAdapterType;
import cz.cas.lib.arcstorage.storage.ceph.CephS3StorageService;
import cz.cas.lib.arcstorage.storage.fs.FsStorageService;
import cz.cas.lib.arcstorage.storage.fs.ZfsStorageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.util.Utils.notNull;
import static cz.cas.lib.arcstorage.util.Utils.parseEnumFromConfig;
import static java.util.Collections.shuffle;

@Service
@Slf4j
public class StorageProvider {

    private String keyFilePath;
    private StorageStore storageStore;
    private int connectionTimeout;
    private ConfigurationStore configurationStore;

    /**
     * Returns storage service according to the database object. The storage is tested for reachability and is updated if
     * the reachability changes.
     *
     * @param storage
     * @return storage service
     * @throws ConfigParserException
     */
    private StorageService createAdapter(Storage storage) throws ConfigParserException {
        StorageService service;
        JsonNode root;
        try {
            root = new ObjectMapper().readTree(storage.getConfig());
        } catch(IOException e) {
            throw new ConfigParserException(e);
        }
        switch(storage.getStorageType()) {
            case FS:
                String rootDirPath = root.at("/rootDirPath").textValue();
                notNull(rootDirPath, () -> new ConfigParserException("rootDirPath string missing in FS storage config"));
                service = new FsStorageService(storage, rootDirPath, keyFilePath, connectionTimeout);
                break;
            case ZFS:
                rootDirPath = root.at("/rootDirPath").textValue();
                notNull(rootDirPath, () -> new ConfigParserException("rootDirPath string missing in FS storage config"));
                service = new ZfsStorageService(storage, rootDirPath, keyFilePath, connectionTimeout);
                break;
            case CEPH:
                CephAdapterType cephAdapterType = parseEnumFromConfig(root, "/adapterType", CephAdapterType.class);
                String userKey = root.at("/userKey").textValue();
                String userSecret = root.at("/userSecret").textValue();
                switch(cephAdapterType) {
                    case S3:
                        String region = root.at("/region").textValue();
                        boolean https = root.at("https").booleanValue();
                        if(userKey == null)
                            throw new ConfigParserException("userKey string missing in CEPH storage config");
                        userSecret = userSecret == null ? "ldap" : userSecret;
                        service = new CephS3StorageService(storage, userKey, userSecret, https, region, connectionTimeout);
                        break;
                    case SWIFT:
                        throw new UnsupportedOperationException();
                    case LIBRADOS:
                        throw new UnsupportedOperationException();
                    default:
                        throw new GeneralException("unknown storage type: " + storage.getStorageType());
                }
                break;
            default:
                throw new GeneralException("unknown storage type: " + storage.getStorageType());
        }
        boolean reachable = service.testConnection();
        if(reachable != storage.isReachable()) {
            storage.setReachable(reachable);
            storageStore.save(storage);
        }
        return service;
    }

    /**
     * Returns all storage services according to the database objects. All storages are tested for reachability and their
     * reachablity flag is updated if changed.
     *
     * @return storage services for all storages
     * @throws SomeLogicalStoragesNotReachableException                       if some storage is unreachable
     * @throws cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException
     */
    public List<StorageService> createAdaptersForWriteOperation() throws SomeLogicalStoragesNotReachableException,
            NoLogicalStorageAttachedException, ReadOnlyStateException {
        if(configurationStore.get().isReadOnly())
            throw new ReadOnlyStateException();
        List<StorageService> storageServices = new ArrayList<>();
        List<Storage> unreachableStorages = new ArrayList<>();
        for(Storage storage : storageStore.findAll()) {
            StorageService service = createAdapter(storage);
            if(!service.getStorage().isReachable()) {
                unreachableStorages.add(storage);
                continue;
            }
            storageServices.add(service);
        }
        if(storageServices.isEmpty())
            throw new NoLogicalStorageAttachedException();
        if(!unreachableStorages.isEmpty())
            throw new SomeLogicalStoragesNotReachableException(unreachableStorages);
        return storageServices;
    }

    /**
     * Returns all storage services according to the database objects. All storages are tested for reachability and their
     * reachablity flag is updated if changed.
     *
     * @return storage services
     * @throws ConfigParserException
     */
    public List<StorageService> createAllAdapters() throws NoLogicalStorageAttachedException {
        List<StorageService> list = storageStore.findAll().stream().map(this::createAdapter).collect(Collectors.toList());
        if(list.isEmpty())
            throw new NoLogicalStorageAttachedException();
        return list;
    }

    /**
     * Returns storage service according to the {@link Storage} with the provided id.
     *
     * @param storageId
     * @return storage service for the storage
     */
    @Transactional
    public StorageService createAdapter(String storageId) {
        Storage storage = storageStore.find(storageId);
        if(storage == null) throw new MissingObject(Storage.class, storageId);
        return createAdapter(storage);
    }

    /**
     * called only by retrieval, GET methods.. methods which writes writes to all storages
     *
     * @return map of reachable and readable storage services sorted by priorities in the descending order (highest priority storages first),
     * where the key is the priority and the value is a list of storages with the given priority
     * @throws NoLogicalStorageReachableException if the number of reachable storages is zero
     * @throws NoLogicalStorageAttachedException  if the number of attached storages is zero
     */
    public List<StorageService> getReachableStorageServicesByPriorities()
            throws NoLogicalStorageReachableException, NoLogicalStorageAttachedException {
        //sorted map where the keys are the priorities and the values are the lists of storage services
        TreeMap<Integer, List<StorageService>> storageServicesByPriorities = new TreeMap<>(Collections.reverseOrder());
        createAllAdapters().forEach(adapter -> {
            if(adapter.getStorage().isReachable() && !adapter.getStorage().isWriteOnly()) {
                List<StorageService> storageServices = storageServicesByPriorities.get(adapter.getStorage().getPriority());
                if(storageServices == null) storageServices = new ArrayList<>();
                storageServices.add(adapter);
                storageServicesByPriorities.put(adapter.getStorage().getPriority(), storageServices);
            }
        });
        if(storageServicesByPriorities.isEmpty()) {
            log.error("there are no logical storages reachable");
            throw new NoLogicalStorageReachableException();
        }
        List<StorageService> orderedShuffledStorageServices = storageServicesByPriorities.values().stream().map(storageServicesByPriority -> {
            shuffle(storageServicesByPriority);
            return storageServicesByPriority;
        })
                .flatMap(List::stream)
                .collect(Collectors.toList());
        return orderedShuffledStorageServices;
    }

    @Inject
    public void setKeyFilePath(@Value("${arcstorage.auth-key}") String keyFilePath) {
        this.keyFilePath = keyFilePath;
    }

    @Inject
    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }

    @Inject
    public void setConnectionTimeout(@Value("${arcstorage.connection-timeout}") String connectionTimeout) {
        this.connectionTimeout = Integer.parseInt(connectionTimeout);
    }

    @Inject
    public void setConfigurationStore(ConfigurationStore configurationStore) {
        this.configurationStore = configurationStore;
    }
}
