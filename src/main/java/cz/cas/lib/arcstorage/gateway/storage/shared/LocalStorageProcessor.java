package cz.cas.lib.arcstorage.gateway.storage.shared;

import cz.cas.lib.arcstorage.domain.AipState;
import cz.cas.lib.arcstorage.domain.ChecksumType;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.gateway.storage.exception.FileCorruptedAfterStoreException;
import cz.cas.lib.arcstorage.gateway.storage.exception.FileDoesNotExistException;
import cz.cas.lib.arcstorage.gateway.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.gateway.storage.exception.StorageException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static cz.cas.lib.arcstorage.gateway.storage.shared.StorageUtils.computeChecksum;
import static cz.cas.lib.arcstorage.gateway.storage.shared.StorageUtils.toXmlId;

public class LocalStorageProcessor implements StorageProcessor {

    private StorageConfig storageConfig;
    private String S;

    public LocalStorageProcessor(StorageConfig storageConfig, String separator) {
        this.storageConfig = storageConfig;
        this.S = separator;
    }

    @Override
    public void storeAip(AipRef aip, AtomicBoolean rollback) throws StorageException {
        Path sipFilePath = getSipPath(aip.getSip().getId());
        Path xmlFilePath = getXmlPath(aip.getSip().getId());
        storeFile(xmlFilePath, toXmlId(aip.getSip().getId(), 1), aip.getXml().getStream(), aip.getXml().getChecksum(), rollback);
        storeFile(sipFilePath, aip.getSip().getId(), aip.getSip().getStream(), aip.getSip().getChecksum(), rollback);
    }

    @Override
    public void storeXml(String sipId, XmlFileRef xml, AtomicBoolean rollback) throws StorageException {
        storeFile(getXmlPath(sipId), toXmlId(sipId, xml.getVersion()), xml.getStream(), xml.getChecksum(), rollback);
    }

    @Override
    public void rollbackAip(String sipId) throws StorageException {
        Path sipPath = getSipPath(sipId);
        try {
            Files.createDirectories(sipPath);
            if (Files.notExists(sipPath.resolve(sipId + ".LOCK")))
                Files.createFile(sipPath.resolve(sipId + ".LOCK"));
            Files.deleteIfExists(sipPath.resolve(sipId));
            Files.deleteIfExists(sipPath.resolve(sipId + ".REMOVED"));
            Files.deleteIfExists(sipPath.resolve(sipId + ".ROLLBACKED"));
            for (ChecksumType checksumType : ChecksumType.values()) {
                Files.deleteIfExists(sipPath.resolve(sipId + "." + checksumType));
            }
            sipPath.resolve(sipId + ".LOCK").toFile().renameTo(sipPath.resolve(sipId + ".ROLLBACKED").toFile());
            rollbackXml(sipId, 1);
        } catch (IOException e) {
            throw new IOStorageException(e);
        }
    }

    @Override
    public void rollbackXml(String sipId, int version) throws StorageException {
        Path xmlPath = getXmlPath(sipId);
        String xmlId = toXmlId(sipId, version);
        try {
            if (Files.notExists(xmlPath.resolve(xmlId + ".LOCK")))
                Files.createFile(xmlPath.resolve(xmlId + ".LOCK"));
            Files.deleteIfExists(xmlPath.resolve(xmlId));
            Files.deleteIfExists(xmlPath.resolve(xmlId + ".ROLLBACKED"));
            for (ChecksumType checksumType : ChecksumType.values()) {
                Files.deleteIfExists(xmlPath.resolve(xmlId + "." + checksumType));
            }
        } catch (IOException e) {
            throw new IOStorageException(e);
        }
        xmlPath.resolve(xmlId + ".LOCK").toFile().renameTo(xmlPath.resolve(xmlId + ".ROLLBACKED").toFile());
    }

    @Override
    public AipStateInfo getAipInfo(String sipId, Checksum sipChecksum, AipState aipState, Map<Integer, Checksum> xmlVersions) throws StorageException {
        Path sipPath = getSipPath(sipId);
        Path xmlPath = getXmlPath(sipId);
        AipStateInfo info = new AipStateInfo(storageConfig.getName(), storageConfig.getStorageType(), aipState);
        if (aipState == AipState.ARCHIVED || aipState == AipState.REMOVED) {
            try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(sipPath.resolve(sipId).toFile()))) {
                Checksum storageSipChecksum = StorageUtils.computeChecksum(bis, sipChecksum.getType());
                info.setChecksum(storageSipChecksum);
                info.setConsistent(sipChecksum.equals(storageSipChecksum));
            } catch (FileNotFoundException e) {
                throw new FileDoesNotExistException(e);
            } catch (IOException e) {
                throw new IOStorageException(e);
            }
        } else {
            info.setChecksum(null);
            info.setConsistent(false);
        }
        for (Integer version : xmlVersions.keySet()) {
            try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(xmlPath.resolve(toXmlId(sipId, version)).toFile()))) {
                Checksum dbChecksum = xmlVersions.get(version);
                Checksum storageChecksum = StorageUtils.computeChecksum(bis, dbChecksum.getType());
                info.addXmlInfo(new XmlStateInfo(version, dbChecksum.equals(storageChecksum), storageChecksum));
            } catch (FileNotFoundException e) {
                throw new FileDoesNotExistException(e);
            } catch (IOException e) {
                throw new IOStorageException(e);
            }
        }
        return info;
    }

    private void storeFile(Path filePath, String id, InputStream stream, Checksum checksum, AtomicBoolean rollback) throws StorageException {
        try {
            Files.createDirectories(filePath);
            Files.createFile(filePath.resolve(id + ".LOCK"));
            try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath.resolve(id).toFile()))) {
                BufferedInputStream bis = new BufferedInputStream(stream);
                byte[] buffer = new byte[1024];
                int read;
                do {
                    if (rollback.get())
                        return;
                    read = bis.read(buffer);
                    bos.write(buffer);
                } while (read > 0);
            }
            Checksum storageXmlChecksum;
            try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(filePath.resolve(id).toFile()))) {
                storageXmlChecksum = computeChecksum(bis, checksum.getType(), rollback);
            }
            if (storageXmlChecksum == null)
                return;
            if (!checksum.equals(storageXmlChecksum)) {
                rollback.set(true);
                throw new FileCorruptedAfterStoreException();
            }
            Files.copy(new ByteArrayInputStream(checksum.getHash().getBytes()), filePath.resolve(id + "." + checksum.getType()));
            Files.delete(filePath.resolve(id + ".LOCK"));
        } catch (FileNotFoundException e) {
            rollback.set(true);
            throw new IOStorageException(e);
        } catch (IOException e) {
            rollback.set(true);
            throw new IOStorageException(e);
        }
    }

    private Path getSipPath(String fileName) {
        return Paths.get(storageConfig.getSipLocation() + S + fileName.substring(0, 2) + S + fileName.substring(2, 4) + S + fileName.substring(4, 6));
    }

    private Path getXmlPath(String fileName) {
        return Paths.get(storageConfig.getXmlLocation() + S + fileName.substring(0, 2) + S + fileName.substring(2, 4) + S + fileName.substring(4, 6));
    }
}