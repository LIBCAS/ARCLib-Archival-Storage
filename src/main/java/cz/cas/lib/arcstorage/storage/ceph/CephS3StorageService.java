package cz.cas.lib.arcstorage.storage.ceph;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import cz.cas.lib.arcstorage.domain.ChecksumType;
import cz.cas.lib.arcstorage.domain.ObjectState;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.StorageUtils;
import cz.cas.lib.arcstorage.storage.exception.FileCorruptedAfterStoreException;
import cz.cas.lib.arcstorage.storage.exception.FileDoesNotExistException;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.input.NullInputStream;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class CephS3StorageService implements StorageService {

    //keys must not contain dash or camelcase
    static final String STATE_KEY = "state";
    static final String CREATED_KEY = "created";

    private StorageConfig storageConfig;
    private String userAccessKey;
    private String userSecretKey;
    private String region;

    public CephS3StorageService(StorageConfig storageConfig, String userAccessKey, String userSecretKey, String region) {
        this.storageConfig = storageConfig;
        this.userAccessKey = userAccessKey;
        this.userSecretKey = userSecretKey;
        this.region = region;
    }

    @Override
    public StorageConfig getStorageConfig() {
        return storageConfig;
    }

    @Override
    public void storeAip(AipDto aipDto, AtomicBoolean rollback) throws StorageException {
        AmazonS3 s3 = connect();
        ArchivalObjectDto sip = aipDto.getSip();
        XmlDto xml = aipDto.getXml();
        storeFile(s3, sip.getId(), sip.getInputStream(), sip.getChecksum(), rollback);
        storeFile(s3, toXmlId(sip.getId(), xml.getVersion()), xml.getInputStream(), xml.getChecksum(), rollback);
    }

    /**
     * Retrieves reference to a file.
     *
     * @param sipId
     * @param xmlVersions specifies which XML versions should be retrieved, typically all or the latest only
     * @return
     * @throws StorageException
     */
    @Override
    public List<FileContentDto> getAip(String sipId, Integer... xmlVersions) throws FileDoesNotExistException {
        AmazonS3 s3 = connect();
        List<FileContentDto> list = new ArrayList<>();
        checkFileExists(s3, sipId);
        list.add(new FileContentDto(s3.getObject(storageConfig.getLocation(), sipId).getObjectContent()));
        for (Integer xmlVersion : xmlVersions) {
            String xmlId = toXmlId(sipId, xmlVersion);
            checkFileExists(s3, xmlId);
            list.add(new FileContentDto(s3.getObject(storageConfig.getLocation(), xmlId).getObjectContent()));
        }
        return list;
    }

    @Override
    public void storeXml(String sipId, XmlDto xmlRef, AtomicBoolean rollback) throws StorageException {
        AmazonS3 s3 = connect();
        storeFile(s3, toXmlId(sipId, xmlRef.getVersion()), xmlRef.getInputStream(), xmlRef.getChecksum(), rollback);
    }

    @Override
    public FileContentDto getXml(String sipId, int version) throws FileDoesNotExistException {
        AmazonS3 s3 = connect();
        String xmlId = toXmlId(sipId, version);
        checkFileExists(s3, xmlId);
        return new FileContentDto(s3.getObject(storageConfig.getLocation(), xmlId).getObjectContent());
    }

    @Override
    public void storeSip(ArchivalObjectDto aipRef, AtomicBoolean rollback) throws StorageException {
        AmazonS3 s3 = connect();
        storeFile(s3, aipRef.getId(), aipRef.getInputStream(), aipRef.getChecksum(), rollback);
    }

    @Override
    public void deleteSip(String sipId) throws StorageException {
        AmazonS3 s3 = connect();
        String metadataId = toMetadataObjectId(sipId);
        checkFileExists(s3, metadataId);
        ObjectMetadata metadata = s3.getObjectMetadata(storageConfig.getLocation(), metadataId);
        metadata.addUserMetadata(STATE_KEY, ObjectState.PROCESSING.toString());
        s3.putObject(storageConfig.getLocation(), toMetadataObjectId(sipId), new NullInputStream(0), metadata);
        s3.deleteObject(storageConfig.getLocation(), sipId);
        metadata.addUserMetadata(STATE_KEY, ObjectState.DELETED.toString());
        s3.putObject(storageConfig.getLocation(), toMetadataObjectId(sipId), new NullInputStream(0), metadata);
    }

    @Override
    public void remove(String sipId) throws StorageException {
        AmazonS3 s3 = connect();
        String metadataId = toMetadataObjectId(sipId);
        checkFileExists(s3, metadataId);
        ObjectMetadata objectMetadata = s3.getObjectMetadata(storageConfig.getLocation(), metadataId);
        objectMetadata.addUserMetadata(STATE_KEY, ObjectState.REMOVED.toString());
        s3.putObject(storageConfig.getLocation(), toMetadataObjectId(sipId), new NullInputStream(0), objectMetadata);
    }

    @Override
    public void rollbackAip(String sipId) throws StorageException {
        AmazonS3 s3 = connect();
        rollbackFile(s3, sipId);
        rollbackFile(s3, toXmlId(sipId, 1));
    }

    @Override
    public void rollbackXml(String sipId, int version) throws StorageException {
        AmazonS3 s3 = connect();
        rollbackFile(s3, toXmlId(sipId, version));
    }

    @Override
    public AipStateInfoDto getAipInfo(String sipId, Checksum sipChecksum, ObjectState objectState, Map<Integer, Checksum> xmlVersions) throws FileDoesNotExistException {
        AmazonS3 s3 = connect();
        AipStateInfoDto info = new AipStateInfoDto(storageConfig.getName(), storageConfig.getStorageType(), objectState, sipChecksum);
        if (objectState == ObjectState.ARCHIVED || objectState == ObjectState.REMOVED) {
            checkFileExists(s3, sipId);
            S3Object sipObject = s3.getObject(storageConfig.getLocation(), sipId);
            Checksum storageFileChecksum = StorageUtils.computeChecksum(sipObject.getObjectContent(),
                    sipChecksum.getType());
            info.setStorageChecksum(storageFileChecksum);
            info.setConsistent(sipChecksum.equals(storageFileChecksum));
        } else {
            info.setStorageChecksum(null);
            info.setConsistent(false);
        }

        for (Integer version : xmlVersions.keySet()) {
            String xmlId = toXmlId(sipId, version);
            checkFileExists(s3, xmlId);
            S3Object xmlObject = s3.getObject(storageConfig.getLocation(), xmlId);
            Checksum dbChecksum = xmlVersions.get(version);
            Checksum storageFileChecksum = StorageUtils.computeChecksum(xmlObject.getObjectContent(),
                    dbChecksum.getType());
            info.addXmlInfo(new XmlStateInfoDto(version, dbChecksum.equals(storageFileChecksum), storageFileChecksum,
                    dbChecksum));
        }
        return info;
    }

    @Override
    public StorageStateDto getStorageState() throws StorageException {
        //bucket quota, user quota, whole cluster, number of objects ...
        throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public boolean testConnection() {
        try {
            AmazonS3 s3 = connect();
            s3.getBucketLocation(storageConfig.getLocation());
        } catch (Exception e) {
            log.error(storageConfig.getName() + " unable to connect: " + e.getClass() + " " + e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * Stores file and then reads it and verifies its fixity.
     * <p>
     * If rollback is set to true by another thread, this method returns ASAP  (without throwing exception). It also tries to delete already stored parts but
     * the cleaning may not be successful. All parts should be cleaned during rollback again.
     * </p>
     * <p>
     * In case of any exception, rollback flag is set to true.
     * </p>
     *
     * @param s3       connection
     * @param id       id of new file
     * @param stream   new file stream
     * @param checksum storageChecksum of the file, this is only added to metadata and not used for fixity comparison, fixity is compared per each chunk during upload
     * @param rollback rollback flag to be periodically checked
     * @throws FileCorruptedAfterStoreException if fixity does not match after store
     * @throws IOStorageException               in case of any {@link IOException}
     * @throws GeneralException                 in case of any unexpected error
     */

    void storeFile(AmazonS3 s3, String id, InputStream stream, Checksum checksum, AtomicBoolean rollback) throws FileCorruptedAfterStoreException, IOStorageException {
        if (rollback.get())
            return;
        try (BufferedInputStream bis = new BufferedInputStream(stream)) {
            InitiateMultipartUploadRequest initReq = new InitiateMultipartUploadRequest(storageConfig.getLocation(), id, new ObjectMetadata());
            InitiateMultipartUploadResult initRes = s3.initiateMultipartUpload(initReq);

            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.addUserMetadata(checksum.getType().toString(), checksum.getHash());
            objectMetadata.addUserMetadata(STATE_KEY, ObjectState.PROCESSING.toString());
            objectMetadata.addUserMetadata(CREATED_KEY, LocalDateTime.now().toString());
            objectMetadata.setContentLength(0);
            PutObjectRequest metadataPutRequest = new PutObjectRequest(storageConfig.getLocation(), toMetadataObjectId(id), new NullInputStream(0), objectMetadata);
            s3.putObject(metadataPutRequest);

            byte[] buff = new byte[8 * 1024 * 1024];
            int read;
            boolean last;
            List<PartETag> partETags = new ArrayList<>();
            int partNumber = 0;
            do {
                if (rollback.get())
                    return;
                partNumber++;
                read = bis.read(buff);
                last = read != buff.length;
                long partSize = Math.min(read, buff.length);
                if (last)
                    buff = Arrays.copyOf(buff, read);
                UploadPartRequest uploadPartRequest = new UploadPartRequest()
                        .withBucketName(storageConfig.getLocation())
                        .withUploadId(initRes.getUploadId())
                        .withKey(id)
                        .withInputStream(new ByteArrayInputStream(buff))
                        .withPartNumber(partNumber)
                        .withPartSize(partSize)
                        .withLastPart(last);
                UploadPartResult uploadPartResult = s3.uploadPart(uploadPartRequest);
                Checksum partChecksum = computeChecksumRollbackAware(new ByteArrayInputStream(buff), ChecksumType.MD5, rollback);
                if (partChecksum == null)
                    return;
                if (!partChecksum.getHash().equalsIgnoreCase(uploadPartResult.getETag()))
                    throw new FileCorruptedAfterStoreException("S3 - part of multipart file", new Checksum(ChecksumType.MD5, uploadPartResult.getETag()), partChecksum);
                partETags.add(uploadPartResult.getPartETag());
            } while (!last);
            CompleteMultipartUploadRequest completeReq = new CompleteMultipartUploadRequest(storageConfig.getLocation(), id, initRes.getUploadId(), partETags);
            s3.completeMultipartUpload(completeReq);
            objectMetadata.addUserMetadata(STATE_KEY, ObjectState.ARCHIVED.toString());
            s3.putObject(metadataPutRequest);
        } catch (Exception e) {
            rollback.set(true);
            if (e instanceof IOException)
                throw new IOStorageException(e);
            if (e instanceof FileCorruptedAfterStoreException)
                throw (FileCorruptedAfterStoreException) e;
            throw new GeneralException(e);
        }
    }

    void rollbackFile(AmazonS3 s3, String id) {
        ObjectMetadata objectMetadata;
        String metadataId = toMetadataObjectId(id);
        boolean metadataExists = s3.doesObjectExist(storageConfig.getLocation(), metadataId);
        if (!metadataExists)
            objectMetadata = new ObjectMetadata();
        else
            objectMetadata = s3.getObjectMetadata(storageConfig.getLocation(), metadataId);
        objectMetadata.addUserMetadata(STATE_KEY, ObjectState.PROCESSING.toString());
        s3.putObject(storageConfig.getLocation(), metadataId, new NullInputStream(0), objectMetadata);
        List<MultipartUpload> multipartUploads = s3.listMultipartUploads(new ListMultipartUploadsRequest(storageConfig.getLocation()).withPrefix(id)).getMultipartUploads();
        if (multipartUploads.size() == 1)
            s3.abortMultipartUpload(new AbortMultipartUploadRequest(storageConfig.getLocation(), id, multipartUploads.get(0).getUploadId()));
        else if (multipartUploads.size() > 1)
            throw new GeneralException("unexpected error during rollback of file: " + id + " : there are more than one upload in progress");
        s3.deleteObject(storageConfig.getLocation(), id);
        objectMetadata.addUserMetadata(STATE_KEY, ObjectState.ROLLBACKED.toString());
        s3.putObject(storageConfig.getLocation(), metadataId, new NullInputStream(0), objectMetadata);
    }

    AmazonS3 connect() {
        AWSCredentials credentials = new BasicAWSCredentials(userAccessKey, userSecretKey);
        AWSStaticCredentialsProvider provider = new AWSStaticCredentialsProvider(credentials);
        ClientConfiguration clientConfig = new ClientConfiguration();
        //force usage of AWS signature v2 instead of v4 to enable multipart uploads (v4 does not work with multipart upload for now)
        clientConfig.setSignerOverride("S3SignerType");
        clientConfig.setProtocol(Protocol.HTTP);
        AmazonS3 conn = AmazonS3ClientBuilder
                .standard()
                .withCredentials(provider)
                .withClientConfiguration(clientConfig)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(storageConfig.getHost() + ":" + storageConfig.getPort(), region))
                .build();
        return conn;
    }

    private void checkFileExists(AmazonS3 s3, String id) throws FileDoesNotExistException {
        boolean exist = s3.doesObjectExist(storageConfig.getLocation(), id);
        if (!exist)
            throw new FileDoesNotExistException("bucket: " + storageConfig.getLocation() + " id: " + id);
    }

    String toMetadataObjectId(String objId) {
        return objId + ".meta";
    }
}
