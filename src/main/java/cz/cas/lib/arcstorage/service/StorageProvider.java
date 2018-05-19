package cz.cas.lib.arcstorage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.service.exception.ConfigParserException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.ceph.CephAdapterType;
import cz.cas.lib.arcstorage.storage.ceph.CephS3StorageService;
import cz.cas.lib.arcstorage.storage.fs.FsStorageService;
import cz.cas.lib.arcstorage.storage.fs.ZfsStorageService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.util.Utils.parseEnumFromConfig;

@Service
public class StorageProvider {

    private String keyFilePath;
    private StorageStore storageStore;

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
        switch (storage.getStorageType()) {
            case FS:
                service = new FsStorageService(storage, keyFilePath);
                break;
            case ZFS:
                service = new ZfsStorageService(storage, keyFilePath);
                break;
            case CEPH:
                try {
                    root = new ObjectMapper().readTree(storage.getConfig());
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
                        service = new CephS3StorageService(storage, userKey, userSecret, region);
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
        if (reachable != storage.isReachable()) {
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
     * @throws SomeLogicalStoragesNotReachableException if some storage is unreachable
     */
    public List<StorageService> createReachableAdapters() throws SomeLogicalStoragesNotReachableException, NoLogicalStorageAttachedException {
        List<StorageService> storageServices = new ArrayList<>();
        List<Storage> unreachableStorages = new ArrayList<>();
        for (Storage storage : storageStore.findAll()) {
            StorageService service = createAdapter(storage);
            if (!service.getStorage().isReachable()) {
                unreachableStorages.add(storage);
                continue;
            }
            storageServices.add(service);
        }
        if (storageServices.isEmpty())
            throw new NoLogicalStorageAttachedException();
        if (!unreachableStorages.isEmpty())
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
        if (list.isEmpty())
            throw new NoLogicalStorageAttachedException();
        return list;
    }

    @Inject
    public void setKeyFilePath(@Value("${arcstorage.auth-key}") String keyFilePath) {
        this.keyFilePath = keyFilePath;
    }

    @Inject
    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }
}
