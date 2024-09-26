package cz.cas.lib.arcstorage.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.domain.store.SystemStateStore;
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
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
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

    private String sshKeyFilePath;
    private String sshUsername;
    private StorageStore storageStore;
    private int connectionTimeout;
    private SystemStateStore systemStateStore;
    private TransactionTemplate transactionTemplate;

    /**
     * Returns storage service according to the database object. The storage is tested for reachability and is updated if
     * the reachability changes.
     *
     * @param storage
     * @param checkReachability
     * @return storage service
     * @throws ConfigParserException
     */
    public StorageService createAdapter(Storage storage, boolean checkReachability) throws ConfigParserException {
        StorageService service;
        JsonNode root;
        try {
            root = new ObjectMapper().readTree(storage.getConfig());
        } catch (JsonProcessingException e) {
            throw new ConfigParserException(e);
        }
        switch (storage.getStorageType()) {
            case FS:
                String rootDirPath = root.at("/rootDirPath").textValue();
                notNull(rootDirPath, () -> new ConfigParserException("rootDirPath string missing in FS storage config"));
                service = new FsStorageService(storage, rootDirPath, sshKeyFilePath, sshUsername, connectionTimeout);
                break;
            case ZFS:
                rootDirPath = root.at("/rootDirPath").textValue();
                notNull(rootDirPath, () -> new ConfigParserException("rootDirPath string missing in FS storage config"));
                String poolName = root.at("/poolName").textValue();
                notNull(poolName, () -> new ConfigParserException("poolName string missing in FS storage config"));
                service = new ZfsStorageService(storage, rootDirPath, poolName, sshKeyFilePath, sshUsername, connectionTimeout);
                break;
            case CEPH:
                CephAdapterType cephAdapterType = parseEnumFromConfig(root, "/adapterType", CephAdapterType.class);
                String userKey = root.at("/userKey").textValue();
                String userSecret = root.at("/userSecret").textValue();
                String cluster = root.at("/cluster").textValue();
                String cephBinHome = root.at("/cephBinHome").textValue();
                switch (cephAdapterType) {
                    case S3:
                        String region = root.at("/region").textValue();
                        String sshServer = root.at("/sshServer").textValue();
                        int sshPort = root.at("/sshPort").intValue();
                        boolean https = root.at("/https").booleanValue();
                        boolean virtualHost = root.at("/virtualHost").booleanValue();
                        if (userKey == null)
                            throw new ConfigParserException("userKey string missing in CEPH storage config");
                        userSecret = userSecret == null ? "ldap" : userSecret;
                        service = new CephS3StorageService(storage, userKey, userSecret, https, region, connectionTimeout, sshServer, sshPort, sshKeyFilePath, sshUsername, virtualHost, cluster, cephBinHome);
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
        if (checkReachability) {
            boolean reachable = service.testConnection();
            if (reachable != storage.isReachable()) {
                storage.setReachable(reachable);
                storageStore.save(storage);
            }
        }
        return service;
    }

    /**
     * Returns all storage services according to the database objects. All storages are tested for reachability and their
     * reachablity flag is updated if changed.
     *
     * @return storage services for all storages
     * @throws SomeLogicalStoragesNotReachableException                                          if some storage is unreachable
     * @throws cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException
     * @throws cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException
     */
    public List<StorageService> createAdaptersForWriteOperation() throws SomeLogicalStoragesNotReachableException,
            NoLogicalStorageAttachedException, ReadOnlyStateException {
        return createAdaptersForWriteOperation(true);
    }

    /**
     * USE WITH CAUTION.. for standard write operations it is better to use {@link StorageProvider#createAdaptersForWriteOperation()} which
     * internally calls this method with checkSystemState attribute set to true
     * <br>
     * this method exists for special cases, such as cleanup, in which case we do allow writing to strage even if system is in read-only mode
     *
     * @param checkSystemState use false in special cases, otherwise don't use this method at all
     * @return
     * @throws SomeLogicalStoragesNotReachableException
     * @throws NoLogicalStorageAttachedException
     * @throws ReadOnlyStateException
     */
    public List<StorageService> createAdaptersForWriteOperation(boolean checkSystemState) throws SomeLogicalStoragesNotReachableException,
            NoLogicalStorageAttachedException, ReadOnlyStateException {
        if (checkSystemState && systemStateStore.get().isReadOnly())
            throw new ReadOnlyStateException();
        Pair<List<StorageService>, List<StorageService>> services = checkReachabilityOfAllStorages();
        if (services.getLeft().isEmpty() && services.getRight().isEmpty())
            throw new NoLogicalStorageAttachedException();
        if (!services.getRight().isEmpty())
            throw new SomeLogicalStoragesNotReachableException(services.getRight().stream().map(StorageService::getStorage).collect(Collectors.toList()));
        return services.getLeft();
    }

    /**
     * Returns all storage services of non-synchronizing storages. All storages are tested for reachability and their
     * reachablity flag is updated if changed.
     *
     * @return storage services for all non-synchronizing storages
     * @throws SomeLogicalStoragesNotReachableException                                          if some non-synchronizing storage is unreachable
     * @throws cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException
     * @throws cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException
     */
    public List<StorageService> createAdaptersForModifyOperation() throws SomeLogicalStoragesNotReachableException,
            NoLogicalStorageAttachedException, ReadOnlyStateException {
        if (systemStateStore.get().isReadOnly())
            throw new ReadOnlyStateException();
        Pair<List<StorageService>, List<StorageService>> services = checkReachabilityOfAllStorages();
        if (services.getLeft().isEmpty() && services.getRight().isEmpty())
            throw new NoLogicalStorageAttachedException();
        if (!services.getRight().isEmpty() && services.getRight().stream().noneMatch(s -> s.getStorage().isSynchronizing()))
            throw new SomeLogicalStoragesNotReachableException(services.getRight().stream().map(s -> s.getStorage()).collect(Collectors.toList()));
        return services.getLeft().stream().filter(s -> !s.getStorage().isSynchronizing()).collect(Collectors.toList());
    }

    /**
     * returns pair with list of reachable (L) and unreachable (R) storages
     *
     * @return
     */
    @Transactional
    public Pair<List<StorageService>, List<StorageService>> checkReachabilityOfAllStorages() {
        List<StorageService> storageServices = new ArrayList<>();
        List<StorageService> unreachableStorageServices = new ArrayList<>();
        for (Storage storage : storageStore.findAll()) {
            StorageService service = createAdapter(storage, true);
            if (!service.getStorage().isReachable()) {
                unreachableStorageServices.add(service);
                continue;
            }
            storageServices.add(service);
        }

        transactionTemplate.executeWithoutResult(t -> {
            SystemState systemState = systemStateStore.get();
            systemState.setLastReachabilityCheck(Instant.now());
            systemStateStore.save(systemState);
        });

        return Pair.of(storageServices, unreachableStorageServices);
    }

    /**
     * Returns storage service according to the {@link Storage} with the provided id. Checks for reachability.
     *
     * @param storageId
     * @return storage service for the storage
     */
    @Transactional
    public StorageService createAdapter(String storageId) {
        Storage storage = storageStore.find(storageId);
        if (storage == null) throw new MissingObject(Storage.class, storageId);
        return createAdapter(storage, true);
    }

    /**
     * called only by retrieval, GET methods..
     *
     * @return map of reachable and readable storage services sorted by priorities in the descending order (highest priority storages first),
     * where the key is the priority and the value is a list of storages with the given priority..
     * <p>storages which are just synchronizing are not returned</p>
     * @throws NoLogicalStorageReachableException if the number of reachable storages is zero
     * @throws NoLogicalStorageAttachedException  if the number of attached storages is zero
     */
    public List<StorageService> createAdaptersForRead()
            throws NoLogicalStorageReachableException, NoLogicalStorageAttachedException {
        //sorted map where the keys are the priorities and the values are the lists of storage services
        TreeMap<Integer, List<StorageService>> storageServicesByPriorities = new TreeMap<>(Collections.reverseOrder());
        List<StorageService> rwAdapters = storageStore.findAll()
                .stream()
                .filter(a -> !a.isSynchronizing())
                .map(s -> createAdapter(s, true))
                .collect(Collectors.toList());
        if (rwAdapters.isEmpty())
            throw new NoLogicalStorageAttachedException();
        rwAdapters.forEach(adapter -> {
            if (adapter.getStorage().isReachable()) {
                List<StorageService> storageServices = storageServicesByPriorities.get(adapter.getStorage().getPriority());
                if (storageServices == null) storageServices = new ArrayList<>();
                storageServices.add(adapter);
                storageServicesByPriorities.put(adapter.getStorage().getPriority(), storageServices);
            }
        });
        if (storageServicesByPriorities.isEmpty()) {
            log.error("there are no logical storages reachable");
            throw new NoLogicalStorageReachableException();
        }
        return storageServicesByPriorities.values().stream().map(storageServicesByPriority -> {
                    shuffle(storageServicesByPriority);
                    return storageServicesByPriority;
                })
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public long getStoragesCount() {
        return storageStore.getCount();
    }

    @Autowired
    public void setSshKeyFilePath(@Value("${arcstorage.ssh.authKey}") String keyFilePath) {
        this.sshKeyFilePath = keyFilePath;
    }

    @Autowired
    public void setSshUsername(@Value("${arcstorage.ssh.userName}") String username) {
        this.sshUsername = username;
    }

    @Autowired
    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }

    @Autowired
    public void setConnectionTimeout(@Value("${arcstorage.connectionTimeout}") String connectionTimeout) {
        this.connectionTimeout = Integer.parseInt(connectionTimeout);
    }

    @Autowired
    public void setSystemStateStore(SystemStateStore systemStateStore) {
        this.systemStateStore = systemStateStore;
    }

    @Autowired
    public void setTransactionTemplate(TransactionTemplate transactionTemplate) {
        this.transactionTemplate = transactionTemplate;
    }
}
