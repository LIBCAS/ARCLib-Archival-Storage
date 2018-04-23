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
import cz.cas.lib.arcstorage.domain.AipState;
import cz.cas.lib.arcstorage.domain.ChecksumType;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.StorageUtils;
import cz.cas.lib.arcstorage.storage.exception.FileCorruptedAfterStoreException;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
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

public class CephS3StorageService implements StorageService {

    //keys must not contain dash or camelcase
    static final String STATE_KEY = "state";
    static final String CREATED_KEY = "created";
    static final String UPLOAD_ID = "upload-id";

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
    public void storeAip(AipRef aipRef, AtomicBoolean rollback) throws StorageException {
        AmazonS3 s3 = connect();
        ArchiveFileRef sip = aipRef.getSip();
        XmlRef xml = aipRef.getXml();
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
    public List<FileRef> getAip(String sipId, Integer... xmlVersions) throws StorageException {
        AmazonS3 s3 = connect();
        List<FileRef> list = new ArrayList<>();
        list.add(new FileRef(s3.getObject(storageConfig.getLocation(), sipId).getObjectContent()));
        for (Integer xmlVersion : xmlVersions) {
            list.add(new FileRef(s3.getObject(storageConfig.getLocation(), toXmlId(sipId, xmlVersion)).getObjectContent()));
        }
        return list;
    }

    @Override
    public void storeXml(String sipId, XmlRef xmlRef, AtomicBoolean rollback) throws StorageException {
        AmazonS3 s3 = connect();
        storeFile(s3, toXmlId(sipId, xmlRef.getVersion()), xmlRef.getInputStream(), xmlRef.getChecksum(), rollback);
    }

    @Override
    public FileRef getXml(String sipId, int version) throws StorageException {
        AmazonS3 s3 = connect();
        return new FileRef(s3.getObject(storageConfig.getLocation(), toXmlId(sipId, version)).getObjectContent());
    }

    @Override
    public void deleteSip(String sipId) throws StorageException {
        AmazonS3 s3 = connect();
        s3.deleteObject(storageConfig.getLocation(), sipId);
        s3.deleteObject(storageConfig.getLocation(), toMetadataObjectId(sipId));
    }

    @Override
    public void remove(String sipId) throws StorageException {
        AmazonS3 s3 = connect();
        S3Object object = s3.getObject(storageConfig.getLocation(), toMetadataObjectId(sipId));
        object.getObjectMetadata().addUserMetadata(STATE_KEY, AipState.REMOVED.toString());
        s3.putObject(storageConfig.getLocation(), toMetadataObjectId(sipId), new NullInputStream(0), object.getObjectMetadata());
    }

    @Override
    public void rollbackAip(String sipId) throws StorageException {
        AmazonS3 s3 = connect();
        s3.deleteObject(storageConfig.getLocation(), sipId);
        S3Object sipObject = s3.getObject(storageConfig.getLocation(), toMetadataObjectId(sipId));
        sipObject.getObjectMetadata().addUserMetadata(STATE_KEY, AipState.ROLLBACKED.toString());
        s3.putObject(storageConfig.getLocation(), toMetadataObjectId(sipId), new NullInputStream(0), sipObject.getObjectMetadata());

        String xmlId = toXmlId(sipId, 1);
        s3.deleteObject(storageConfig.getLocation(), xmlId);
        S3Object xmlObject = s3.getObject(storageConfig.getLocation(), toMetadataObjectId(xmlId));
        xmlObject.getObjectMetadata().addUserMetadata(STATE_KEY, AipState.ROLLBACKED.toString());
        s3.putObject(storageConfig.getLocation(), toMetadataObjectId(xmlId), new NullInputStream(0), xmlObject.getObjectMetadata());
    }

    @Override
    public void rollbackXml(String sipId, int version) throws StorageException {
        AmazonS3 s3 = connect();
        String xmlId = toXmlId(sipId, version);
        s3.deleteObject(storageConfig.getLocation(), xmlId);
        S3Object object = s3.getObject(storageConfig.getLocation(), toMetadataObjectId(xmlId));
        object.getObjectMetadata().addUserMetadata(STATE_KEY, AipState.ROLLBACKED.toString());
        s3.putObject(storageConfig.getLocation(), toMetadataObjectId(xmlId), new NullInputStream(0), object.getObjectMetadata());
    }

    @Override
    public AipStateInfo getAipInfo(String sipId, Checksum sipChecksum, AipState aipState, Map<Integer, Checksum> xmlVersions) throws StorageException {
        AmazonS3 s3 = connect();
        AipStateInfo info = new AipStateInfo(storageConfig.getName(), storageConfig.getStorageType(), aipState);
        if (aipState == AipState.ARCHIVED || aipState == AipState.REMOVED) {
            S3Object sipObject = s3.getObject(storageConfig.getLocation(), sipId);
            Checksum storageFileChecksum = StorageUtils.computeChecksum(sipObject.getObjectContent(), sipChecksum.getType());
            info.setChecksum(storageFileChecksum);
            info.setConsistent(sipChecksum.equals(storageFileChecksum));
        } else {
            info.setChecksum(null);
            info.setConsistent(false);
        }

        for (Integer version : xmlVersions.keySet()) {
            S3Object xmlObject = s3.getObject(storageConfig.getLocation(), toXmlId(sipId, version));
            Checksum dbChecksum = xmlVersions.get(version);
            Checksum storageFileChecksum = StorageUtils.computeChecksum(xmlObject.getObjectContent(), dbChecksum.getType());
            info.addXmlInfo(new XmlStateInfo(version, dbChecksum.equals(storageFileChecksum), storageFileChecksum));
        }
        return info;
    }

    @Override
    public StorageState getStorageState() throws StorageException {
        return new StorageState(storageConfig);
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
     * @param checksum checksum of the file, this is only added to metadata and not used for fixity comparison, fixity is compared per each chunk during upload
     * @param rollback rollback flag to be periodically checked
     * @throws FileCorruptedAfterStoreException if fixity does not match after store
     * @throws IOStorageException               in case of any {@link IOException}
     * @throws GeneralException                 in case of any unexpected error
     */

    void storeFile(AmazonS3 s3, String id, InputStream stream, Checksum checksum, AtomicBoolean rollback) throws FileCorruptedAfterStoreException, IOStorageException {
        if(rollback.get())
            return;
        InitiateMultipartUploadResult initRes = null;
        try (BufferedInputStream bis = new BufferedInputStream(stream)) {
            InitiateMultipartUploadRequest initReq = new InitiateMultipartUploadRequest(storageConfig.getLocation(), id, new ObjectMetadata());
            initRes = s3.initiateMultipartUpload(initReq);

            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.addUserMetadata(checksum.getType().toString(), checksum.getHash());
            objectMetadata.addUserMetadata(STATE_KEY, AipState.PROCESSING.toString());
            objectMetadata.addUserMetadata(CREATED_KEY, LocalDateTime.now().toString());
            objectMetadata.addUserMetadata(UPLOAD_ID, initRes.getUploadId());
            objectMetadata.setContentLength(0);
            PutObjectRequest metadataPutRequest = new PutObjectRequest(storageConfig.getLocation(), toMetadataObjectId(id), new NullInputStream(0), objectMetadata);
            s3.putObject(metadataPutRequest);

            byte[] buff = new byte[8 * 1024 * 1024];
            int read;
            boolean last;
            List<PartETag> partETags = new ArrayList<>();
            int partNumber = 0;
            do {
                if (rollback.get()) {
                    s3.abortMultipartUpload(new AbortMultipartUploadRequest(storageConfig.getLocation(), id, initRes.getUploadId()));
                    return;
                }
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
                if (partChecksum == null) {
                    s3.abortMultipartUpload(new AbortMultipartUploadRequest(storageConfig.getLocation(), id, initRes.getUploadId()));
                    return;
                }
                if (!partChecksum.getHash().equalsIgnoreCase(uploadPartResult.getETag()))
                    throw new FileCorruptedAfterStoreException("S3 - part of multipart file", new Checksum(ChecksumType.MD5, uploadPartResult.getETag()), partChecksum);
                partETags.add(uploadPartResult.getPartETag());
            } while (!last);
            CompleteMultipartUploadRequest completeReq = new CompleteMultipartUploadRequest(storageConfig.getLocation(), id, initRes.getUploadId(), partETags);
            s3.completeMultipartUpload(completeReq);
            objectMetadata.addUserMetadata(STATE_KEY, AipState.ARCHIVED.toString());
            s3.putObject(metadataPutRequest);
        } catch (Exception e) {
            rollback.set(true);
            if (initRes != null)
                s3.abortMultipartUpload(new AbortMultipartUploadRequest(storageConfig.getLocation(), id, initRes.getUploadId()));
            if (e instanceof IOException)
                throw new IOStorageException(e);
            if (e instanceof FileCorruptedAfterStoreException)
                throw (FileCorruptedAfterStoreException) e;
            throw new GeneralException(e);
        }
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

    String toMetadataObjectId(String objId) {
        return objId + ".meta";
    }
}
