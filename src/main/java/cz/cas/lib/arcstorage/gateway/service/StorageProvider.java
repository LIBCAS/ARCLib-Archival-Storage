package cz.cas.lib.arcstorage.gateway.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.exception.ConfigParserException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.ceph.CephAdapterType;
import cz.cas.lib.arcstorage.storage.ceph.CephS3StorageService;
import cz.cas.lib.arcstorage.storage.fs.FsStorageService;
import cz.cas.lib.arcstorage.storage.fs.ZfsStorageService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.IOException;

import static cz.cas.lib.arcstorage.util.Utils.parseEnumFromConfig;

@Service
public class StorageProvider {

    private String keyFilePath;

    public StorageService createAdapter(StorageConfig storageConfig) throws ConfigParserException {
        switch (storageConfig.getStorageType()) {
            case FS:
                return new FsStorageService(storageConfig, keyFilePath);
            case ZFS:
                return new ZfsStorageService(storageConfig, keyFilePath);
            case CEPH:
                JsonNode root;
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
                            throw new ConfigParserException("userKey or userSecret string missing in storage config");
                        return new CephS3StorageService(storageConfig, userKey, userSecret, region);
                    case SWIFT:
                        throw new UnsupportedOperationException();
                    case LIBRADOS:
                        throw new UnsupportedOperationException();
                }
                break;
        }
        return null;
    }

    @Inject
    public void setKeyFilePath(@Value("${arcstorage.auth-key}") String keyFilePath) {
        this.keyFilePath = keyFilePath;
    }
}
