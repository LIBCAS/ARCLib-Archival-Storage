package cz.cas.lib.arcstorage.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.dto.ChecksumType;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.service.exception.ConfigParserException;
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
import java.util.*;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.util.Utils.notNull;
import static cz.cas.lib.arcstorage.util.Utils.parseEnumFromConfig;

@Service
@Slf4j
public class StorageProvider {

    private static final TypeReference<HashMap<ChecksumType, String>> JSON_CONFIG_CHECKSUM_NODE_TYPE_REF = new TypeReference<>() {
    };

    private String sshKeyFilePath;
    private String sshUsername;
    private StorageStore storageStore;
    private int connectionTimeout;
    private TransactionTemplate transactionTemplate;
    private SystemStateService systemStateService;

    public StorageService createPrimaryStorageAdapter() throws SomeLogicalStoragesNotReachableException {
        SystemState systemState = systemStateService.get();
        StorageService adapter = createAdapter(systemState.getPrimaryStorage());
        if (!adapter.getStorage().isReachable()) {
            SomeLogicalStoragesNotReachableException ex = new SomeLogicalStoragesNotReachableException(adapter.getStorage());
            systemStateService.setReadOnly(systemState, ex);
            throw ex;
        }
        return adapter;
    }

    public Storage getPrimaryStorage() {
        return systemStateService.get().getPrimaryStorage();
    }

    public Set<Storage> getSecondaryStorages() {
        Storage primaryStorage = getPrimaryStorage();
        return storageStore.findAll().stream().filter(s -> !Objects.equals(primaryStorage, s)).collect(Collectors.toSet());
    }

    public Set<Storage> getAllStorages() {
        return new HashSet<>(storageStore.findAll());
    }

    /**
     * checks for reachability of storages
     *
     * @param omitDetached if true then detached storages are skipped: neither checked for reachability, nor returned in the result pair
     * @return pair with list of reachable (L) and unreachable (R) storages
     */
    @Transactional
    public Pair<List<StorageService>, List<StorageService>> checkReachabilityOfAllStorages(boolean omitDetached) {
        List<StorageService> storageServices = new ArrayList<>();
        List<StorageService> unreachableStorageServices = new ArrayList<>();
        for (Storage storage : storageStore.findAll()) {
            if (omitDetached && storage.isDetached()) {
                continue;
            }
            StorageService service = createAdapter(storage);
            if (!service.getStorage().isReachable()) {
                unreachableStorageServices.add(service);
                continue;
            }
            storageServices.add(service);
        }

        if (!omitDetached) {
            transactionTemplate.executeWithoutResult(t -> {
                SystemState systemState = systemStateService.get();
                systemState.setLastReachabilityCheck(Instant.now());
                systemStateService.save(systemState);
            });
        }

        return Pair.of(storageServices, unreachableStorageServices);
    }

    /**
     * Returns storage service according to the {@link Storage} with the provided id. Checks for reachability.
     *
     * @param storageId ID of the storage
     * @return storage service for the storage
     */
    @Transactional
    public StorageService createAdapter(String storageId) {
        Storage storage = storageStore.find(storageId);
        if (storage == null) throw new MissingObject(Storage.class, storageId);
        return createAdapter(storage);
    }

    /**
     * Returns storage service according to the {@link Storage} with the provided id. Checks for reachability.
     *
     * @param storage
     * @return storage service for the storage
     */
    @Transactional
    public StorageService createAdapter(Storage storage) {
        return createAdapter(storage, true);
    }

    /**
     * called only by retrieval, GET methods..
     *
     * @return list of attached, reachable and readable storage services sorted by priorities in the descending order (highest priority storages first),
     * where the key is the priority and the value is a list of storages with the given priority..
     * <p>if {@link SystemState#primaryStorage} is set then it is always the most prioritized</p>
     * <p>storages which are detached are not returned</p>
     * @throws NoLogicalStorageReachableException if the number of reachable storages is zero
     * @throws NoLogicalStorageAttachedException  if the number of attached storages is zero
     */
    public List<StorageService> createAdaptersForRead()
            throws NoLogicalStorageReachableException, NoLogicalStorageAttachedException, SomeLogicalStoragesNotReachableException {
        //sorted map where the keys are the priorities and the values are the lists of storage services
        TreeMap<Integer, List<StorageService>> storageServicesByPriorities = new TreeMap<>(Collections.reverseOrder());
        List<StorageService> adapters = storageStore.findAll()
                .stream()
                .filter(a -> !a.isDetached())
                .map(this::createAdapter)
                .toList();
        if (adapters.isEmpty())
            throw new NoLogicalStorageAttachedException();
        adapters.forEach(adapter -> {
            if (adapter.getStorage().isReachable()) {
                storageServicesByPriorities.computeIfAbsent(adapter.getStorage().getPriority(), pri -> new ArrayList<>()).add(adapter);
            }
        });
        if (storageServicesByPriorities.isEmpty()) {
            log.error("there are no logical storages reachable for read");
            throw new NoLogicalStorageReachableException();
        }
        List<StorageService> storageServices = storageServicesByPriorities.values().stream().peek(Collections::shuffle)
                .flatMap(List::stream)
                .toList();
        //if primary storage is set it is always first no matter the priority attribute
        Storage primaryStorage = getPrimaryStorage();
        if (primaryStorage != null) {
            StorageService primaryService = storageServices.stream().filter(s -> Objects.equals(s.getStorage(), primaryStorage)).findFirst()
                    .orElseThrow(() -> new SomeLogicalStoragesNotReachableException(primaryStorage));
            storageServices = new ArrayList<>(storageServices);
            storageServices.remove(primaryService);
            storageServices.add(0, primaryService);
        }
        return storageServices;
    }

    /**
     * Returns storage service according to the database object. The storage is tested for reachability and is updated if
     * the reachability changes.
     *
     * @param storage           databasse object
     * @param checkReachability whether to check for reachability
     * @return storage service
     * @throws ConfigParserException if storage json config could not be parsed
     */
    public StorageService createAdapter(Storage storage, boolean checkReachability) throws ConfigParserException {
        StorageService service;
        JsonNode root;
        ObjectMapper om = new ObjectMapper();
        try {
            root = om.readTree(storage.getConfig());
        } catch (JsonProcessingException e) {
            throw new ConfigParserException(e);
        }
        switch (storage.getStorageType()) {
            case FS -> {
                String rootDirPath = root.at("/rootDirPath").textValue();
                notNull(rootDirPath, () -> new ConfigParserException("rootDirPath string missing in FS storage config"));
                JsonNode checksumMapNode = root.at("/checksumCmd");
                Map<ChecksumType, String> checskumMap = checksumMapNode == null ? null : om.convertValue(checksumMapNode, JSON_CONFIG_CHECKSUM_NODE_TYPE_REF);
                service = new FsStorageService(storage, rootDirPath, sshKeyFilePath, sshUsername, connectionTimeout, checskumMap);
            }
            case ZFS -> {
                String rootDirPath = root.at("/rootDirPath").textValue();
                notNull(rootDirPath, () -> new ConfigParserException("rootDirPath string missing in FS storage config"));
                JsonNode checksumMapNode = root.at("/checksumCmd");
                Map<ChecksumType, String> checskumMap = checksumMapNode == null ? null : om.convertValue(checksumMapNode, JSON_CONFIG_CHECKSUM_NODE_TYPE_REF);
                String poolName = root.at("/poolName").textValue();
                notNull(poolName, () -> new ConfigParserException("poolName string missing in FS storage config"));
                service = new ZfsStorageService(storage, rootDirPath, poolName, sshKeyFilePath, sshUsername, connectionTimeout, checskumMap);
            }
            case CEPH -> {
                CephAdapterType cephAdapterType = parseEnumFromConfig(root, "/adapterType", CephAdapterType.class);
                String userKey = root.at("/userKey").textValue();
                String userSecret = root.at("/userSecret").textValue();
                String cluster = root.at("/cluster").textValue();
                String cephBinHome = root.at("/cephBinHome").textValue();
                switch (cephAdapterType) {
                    case S3 -> {
                        String region = root.at("/region").textValue();
                        String sshServer = root.at("/sshServer").textValue();
                        int sshPort = root.at("/sshPort").intValue();
                        boolean https = root.at("/https").booleanValue();
                        boolean virtualHost = root.at("/virtualHost").booleanValue();
                        if (userKey == null)
                            throw new ConfigParserException("userKey string missing in CEPH storage config");
                        userSecret = userSecret == null ? "ldap" : userSecret;
                        service = new CephS3StorageService(storage, userKey, userSecret, https, region, connectionTimeout, sshServer, sshPort, sshKeyFilePath, sshUsername, virtualHost, cluster, cephBinHome);
                    }
                    case SWIFT, LIBRADOS -> throw new UnsupportedOperationException();
                    default -> throw new GeneralException("unknown storage type: " + storage.getStorageType());
                }
            }
            default -> throw new GeneralException("unknown storage type: " + storage.getStorageType());
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
    public void setConnectionTimeout(@Value("${arcstorage.connectionTimeout}") int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    @Autowired
    public void setTransactionTemplate(TransactionTemplate transactionTemplate) {
        this.transactionTemplate = transactionTemplate;
    }

    @Autowired
    public void setSystemStateService(SystemStateService systemStateService) {
        this.systemStateService = systemStateService;
    }
}
