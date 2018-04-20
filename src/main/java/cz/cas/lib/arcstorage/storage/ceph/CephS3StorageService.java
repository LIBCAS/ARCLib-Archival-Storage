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
import cz.cas.lib.arcstorage.storage.exception.FileCorruptedAfterStoreException;
import cz.cas.lib.arcstorage.storage.exception.FileDoesNotExistException;
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
    protected static final String STATE_KEY = "state";
    protected static final String CREATED_KEY = "created";
    protected static final String UPLOAD_ID = "upload-id";

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

    @Override
    public List<FileRef> getAip(String sipId, Integer... xmlVersions) throws FileDoesNotExistException, StorageException {
        return null;
    }

    @Override
    public void storeXml(String sipId, XmlRef xmlFileRef, AtomicBoolean rollback) throws StorageException {

    }

    @Override
    public FileRef getXml(String sipId, int version) throws StorageException {
        return null;
    }

    @Override
    public void deleteSip(String id) throws StorageException {

    }

    @Override
    public void remove(String id) throws StorageException {

    }

    @Override
    public void rollbackAip(String sipId) throws StorageException {

    }

    @Override
    public void rollbackXml(String sipId, int version) throws StorageException {

    }

    @Override
    public AipStateInfo getAipInfo(String sipId, Checksum sipChecksum, AipState aipState, Map<Integer, Checksum> xmlVersions) throws StorageException {
        return null;
    }

    @Override
    public StorageState getStorageState() throws StorageException {
        return null;
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
     * @param checksum checksum of the file, this is only added to metadata and not used for fixity comparison
     * @param rollback rollback flag to be periodically checked
     * @throws FileCorruptedAfterStoreException if fixity does not match after store
     * @throws IOStorageException               in case of any {@link IOException}
     * @throws GeneralException                 in case of any unexpected error
     */

    public void storeFile(AmazonS3 s3, String id, InputStream stream, Checksum checksum, AtomicBoolean rollback) throws FileCorruptedAfterStoreException, IOStorageException {
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

    public AmazonS3 connect() {
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

    protected String toMetadataObjectId(String objId) {
        return objId + ".meta";
    }
}
