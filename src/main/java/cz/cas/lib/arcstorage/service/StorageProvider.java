package cz.cas.lib.arcstorage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.cas.lib.arcstorage.domain.entity.StorageConfig;
import cz.cas.lib.arcstorage.service.exception.ConfigParserException;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.service.exception.StorageNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.ceph.CephAdapterType;
import cz.cas.lib.arcstorage.storage.ceph.CephS3StorageService;
import cz.cas.lib.arcstorage.storage.fs.FsStorageService;
import cz.cas.lib.arcstorage.storage.fs.ZfsStorageService;
import cz.cas.lib.arcstorage.domain.store.StorageConfigStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static cz.cas.lib.arcstorage.util.Utils.parseEnumFromConfig;

@Service
public class StorageProvider {

    private String keyFilePath;
    private StorageConfigStore storageConfigStore;

    /**
     * Returns storage service according to storage config. If the storage is not reachable, config is updated and null returned.
     *
     * @param storageConfig
     * @return storage service or null if the storage is not reachable
     * @throws ConfigParserException
     */
    public StorageService createAdapter(StorageConfig storageConfig) throws ConfigParserException {
        StorageService service;
        JsonNode root;
        switch (storageConfig.getStorageType()) {
            case FS:
                service = new FsStorageService(storageConfig, keyFilePath);
                break;
            case ZFS:
                try {
                    root = new ObjectMapper().readTree(storageConfig.getConfig());
                } catch (IOException e) {
                    throw new ConfigParserException(e);
                }
                String pool = root.at("/pool").textValue();
                String dataset = root.at("/dataset").textValue();
                if (pool == null || dataset == null)
                    throw new ConfigParserException("pool or dataset string missing in ZFS storage config");
                service = new ZfsStorageService(storageConfig, pool, dataset, keyFilePath);
                break;
            case CEPH:
                try {
                    root = new ObjectMapper().readTree(storageConfig.getConfig());
                } catch (IOException e) {
                    throw new ConfigParserException(e);
                }
                CephAdapterType cephAdapterType = parseEnumFromConfig(root, "/adapterType", CephAdapterType.class);
                String userKey = root.at("/userKey").textValue();
                String userSecret = root.at("/userSecret").textValue();
                switch (cephAdapterType) {
                    case S3:
                        String region = root.at("/region").textValue();
                        if (userKey == null || userSecret == null)
                            throw new ConfigParserException("userKey or userSecret string missing in CEPH storage config");
                        service = new CephS3StorageService(storageConfig, userKey, userSecret, region);
                        break;
                    case SWIFT:
                        throw new UnsupportedOperationException();
                    case LIBRADOS:
                        throw new UnsupportedOperationException();
                    default:
                        throw new GeneralException("unknown storage type: " + storageConfig.getStorageType());
                }
                break;
            default:
                throw new GeneralException("unknown storage type: " + storageConfig.getStorageType());
        }
        boolean reachable = service.testConnection();
        storageConfig.setReachable(reachable);
        storageConfigStore.save(storageConfig);
        if (!reachable)
            return null;
        return service;
    }

    /**
     * Returns all storage services or throw exception if any of them is unreachable.
     *
     * @return storage services for all storages
     * @throws StorageNotReachableException
     */
    public List<StorageService> createReachableAdapters() throws StorageNotReachableException {
        List<StorageService> storageServices = new ArrayList<>();
        List<StorageConfig> unreachableStorages = new ArrayList<>();
        for (StorageConfig storageConfig : storageConfigStore.findAll()) {
            StorageService service = createAdapter(storageConfig);
            if (service == null) {
                unreachableStorages.add(storageConfig);
                continue;
            }
            storageServices.add(service);
        }
        if (unreachableStorages.isEmpty())
            throw new StorageNotReachableException(unreachableStorages);
        return storageServices;
    }

    @Inject
    public void setKeyFilePath(@Value("${arcstorage.auth-key}") String keyFilePath) {
        this.keyFilePath = keyFilePath;
    }

    @Inject
    public void setStorageConfigStore(StorageConfigStore storageConfigStore) {
        this.storageConfigStore = storageConfigStore;
    }
}
