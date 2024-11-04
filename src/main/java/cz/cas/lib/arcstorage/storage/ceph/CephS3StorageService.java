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
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.StorageUtils;
import cz.cas.lib.arcstorage.storage.exception.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import org.apache.commons.io.input.NullInputStream;

import java.io.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static cz.cas.lib.arcstorage.util.Utils.fetchDataFromRemote;

/**
 * Implementation of {@link StorageService} for the Ceph accessed over S3.
 * <p>
 * Metadata are stored in a separate metadata object, which id is idOfTheObject.meta
 * </p>
 * Fulfillment of the requirements on the metadata storing specified by the interface:
 * <ul>
 * <li>initial checksum of the object: stored in metadata object</li>
 * <li>creation time of the object: stored in metadata object as epoch second</li>
 * <li>state of object matching {@link ObjectState}: stored in metadata object</li>
 * <li>for AIP XML its version and ID of SIP: id of XML is in form aipId_xml_versionNumber</li>
 * </ul>
 */
@Slf4j
public class CephS3StorageService implements StorageService {

    public static final String CMD_STATUS = "ceph -s";
    public static final String CMD_DF = "ceph df";
    public static final String CMD_PGS = "ceph pg ls-by-pool";

    //keys must not contain dash or camelcase
    static final String STATE_KEY = "state";
    static final String CREATED_KEY = "created";

    private Storage storage;
    private String userAccessKey;
    private String userSecretKey;
    private int connectionTimeout;
    private String sshServer;
    private int sshPort;
    private boolean https;
    private boolean virtualHost;
    private String sshKeyFilePath;
    private String sshUserName;
    private String cluster;
    private String cephBinHome;
    private String region;

    public CephS3StorageService(Storage storage,
                                String userAccessKey,
                                String userSecretKey,
                                boolean https,
                                String region,
                                int connectionTimeout,
                                String sshServer,
                                int sshPort,
                                String sshKeyFilePath,
                                String sshUserName,
                                boolean virtualHost,
                                String cluster,
                                String cephBinHome) {
        this.storage = storage;
        this.userAccessKey = userAccessKey;
        this.userSecretKey = userSecretKey;
        this.region = region;
        this.connectionTimeout = connectionTimeout;
        this.https = https;
        this.sshServer = sshServer == null ? storage.getHost() : sshServer;
        this.sshPort = sshPort;
        this.sshKeyFilePath = sshKeyFilePath;
        this.sshUserName = sshUserName;
        this.virtualHost = virtualHost;
        this.cluster = cluster;
        this.cephBinHome = cephBinHome;
    }

    @Override
    public Storage getStorage() {
        return storage;
    }

    @Override
    public void storeAip(AipDto aipDto, AtomicBoolean rollback, String dataSpace) throws StorageException {
        AmazonS3 s3 = connect();
        ArchivalObjectDto sip = aipDto.getSip();
        ArchivalObjectDto xml = aipDto.getXml();
        storeFile(s3, sip.getDatabaseId(), sip.getInputStream(), sip.getChecksum(), rollback, dataSpace, sip.getCreated());
        storeFile(s3, xml.getStorageId(), xml.getInputStream(), xml.getChecksum(), rollback, dataSpace, xml.getCreated());
    }

    @Override
    public AipRetrievalResource getAip(String aipId, String dataSpace, Integer... xmlVersions) throws FileDoesNotExistException {
        AmazonS3 s3 = connect();
        checkFileExists(s3, aipId, dataSpace);
        AipRetrievalResource aip = new AipRetrievalResource(new ClosableS3(s3));
        aip.setSip(s3.getObject(dataSpace, aipId).getObjectContent());
        for (Integer xmlVersion : xmlVersions) {
            String xmlId = toXmlId(aipId, xmlVersion);
            checkFileExists(s3, xmlId, dataSpace);
            aip.addXml(xmlVersion, s3.getObject(dataSpace, xmlId).getObjectContent());
        }
        return aip;
    }

    @Override
    public void storeObject(ArchivalObjectDto objectDto, AtomicBoolean rollback, String dataSpace) throws StorageException {
        try {
            AmazonS3 s3 = connect();
            String id = objectDto.getStorageId();
            switch (objectDto.getState()) {
                case DELETION_FAILURE:
                    storeMetadata(s3, id, objectDto.getChecksum(), ObjectState.DELETED, dataSpace, objectDto.getCreated());
                    break;
                case ARCHIVAL_FAILURE:
                case ROLLBACK_FAILURE:
                    storeMetadata(s3, id, objectDto.getChecksum(), ObjectState.ROLLED_BACK, dataSpace, objectDto.getCreated());
                    break;
                case ROLLED_BACK:
                case DELETED:
                    storeMetadata(s3, id, objectDto.getChecksum(), objectDto.getState(), dataSpace, objectDto.getCreated());
                    break;
                case REMOVED:
                    storeFile(s3, objectDto.getStorageId(), objectDto.getInputStream(), objectDto.getChecksum(), rollback, dataSpace, objectDto.getCreated());
                    remove(objectDto, dataSpace, false);
                    break;
                case ARCHIVED:
                case PROCESSING:
                    storeFile(s3, objectDto.getStorageId(), objectDto.getInputStream(), objectDto.getChecksum(), rollback, dataSpace, objectDto.getCreated());
                    break;
                default:
                    throw new IllegalStateException(objectDto.toString());
            }
        } catch (Exception e) {
            rollback.set(true);
            throw e;
        }
    }

    @Override
    public void storeObjectMetadata(ArchivalObjectDto objectDto, String dataSpace) {
        AmazonS3 s3 = connect();
        storeMetadata(s3, objectDto.getStorageId(), objectDto.getChecksum(), objectDto.getState(), dataSpace, objectDto.getCreated());
    }

    @Override
    public ObjectRetrievalResource getObject(String id, String dataSpace) throws FileDoesNotExistException {
        AmazonS3 s3 = connect();
        checkFileExists(s3, id, dataSpace);
        return new ObjectRetrievalResource(
                s3.getObject(dataSpace, id).getObjectContent(),
                new ClosableS3(s3));
    }

    @Override
    public void delete(ArchivalObjectDto sipDto, String dataSpace, boolean createMetaFileIfMissing) throws StorageException {
        if (createMetaFileIfMissing) {
            throw new UnsupportedOperationException("not implemented yet");
        }
        AmazonS3 s3 = connect();
        String metadataId = toMetadataObjectId(sipDto.getStorageId());
        ObjectMetadata oldMetadata = s3.getObjectMetadata(dataSpace, metadataId);
        oldMetadata.addUserMetadata(STATE_KEY, ObjectState.DELETED.toString());
        ObjectMetadata newMetadata = new ObjectMetadata();
        newMetadata.setUserMetadata(oldMetadata.getUserMetadata());
        s3.putObject(dataSpace, toMetadataObjectId(sipDto.getStorageId()), new NullInputStream(0), newMetadata);
        s3.deleteObject(dataSpace, sipDto.getStorageId());
    }

    @Override
    public void remove(ArchivalObjectDto sipDto, String dataSpace, boolean createMetaFileIfMissing) throws StorageException {
        if (createMetaFileIfMissing) {
            throw new UnsupportedOperationException("not implemented yet");
        }
        AmazonS3 s3 = connect();
        String metadataId = toMetadataObjectId(sipDto.getStorageId());
        ObjectMetadata oldMetadata = s3.getObjectMetadata(dataSpace, metadataId);
        oldMetadata.addUserMetadata(STATE_KEY, ObjectState.REMOVED.toString());
        ObjectMetadata newMetadata = new ObjectMetadata();
        newMetadata.setUserMetadata(oldMetadata.getUserMetadata());
        s3.putObject(dataSpace, toMetadataObjectId(sipDto.getStorageId()), new NullInputStream(0), newMetadata);
    }

    @Override
    public void renew(ArchivalObjectDto sipDto, String dataSpace, boolean createMetaFileIfMissing) throws StorageException {
        if (createMetaFileIfMissing) {
            throw new UnsupportedOperationException("not implemented yet");
        }
        AmazonS3 s3 = connect();
        String metadataId = toMetadataObjectId(sipDto.getStorageId());
        ObjectMetadata oldMetadata = s3.getObjectMetadata(dataSpace, metadataId);
        oldMetadata.addUserMetadata(STATE_KEY, ObjectState.ARCHIVED.toString());
        ObjectMetadata newMetadata = new ObjectMetadata();
        newMetadata.setUserMetadata(oldMetadata.getUserMetadata());
        s3.putObject(dataSpace, toMetadataObjectId(sipDto.getStorageId()), new NullInputStream(0), newMetadata);
    }


    @Override
    public void rollbackAip(AipDto aipDto, String dataSpace) {
        AmazonS3 s3 = connect();
        rollbackFile(s3, aipDto.getSip(), dataSpace);
        for (ArchivalObjectDto xml : aipDto.getXmls()) {
            rollbackFile(s3, xml, dataSpace);
        }
    }

    @Override
    public void rollbackObject(ArchivalObjectDto dto, String dataSpace) throws StorageException {
        AmazonS3 s3 = connect();
        rollbackFile(s3, dto, dataSpace);
    }

    @Override
    public void forgetObject(String objectIdAtStorage, String dataSpace, Instant forgetAuditTimestamp) throws StorageException {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public AipConsistencyVerificationResultDto getAipInfo(ArchivalObjectDto aip, Map<Integer, ArchivalObjectDto> xmls, String dataSpace) throws FileDoesNotExistException {
        AmazonS3 s3 = connect();
        AipConsistencyVerificationResultDto aipStateInfo = new AipConsistencyVerificationResultDto(storage.getName(), storage.getStorageType(), true);
        aipStateInfo.setAipState(fillObjectStateInfo(s3, new ObjectConsistencyVerificationResultDto(), aip, dataSpace));
        for (Integer version : xmls.keySet()) {
            xmls.get(version);
            XmlConsistencyVerificationResultDto info = new XmlConsistencyVerificationResultDto();
            info.setVersion(version);
            fillObjectStateInfo(s3, info, xmls.get(version), dataSpace);
            aipStateInfo.addXmlInfo(info);
        }
        return aipStateInfo;
    }

    @Override
    public StorageStateDto getStorageState() throws StorageException {
        Map<String, Object> storageStateData = new HashMap<>();
        AmazonS3 s3 = connect();
        Owner s3AccountOwner = s3.getS3AccountOwner();
        storageStateData.put("accountId", s3AccountOwner.getId());
        storageStateData.put("accountName", s3AccountOwner.getDisplayName());
        List<BucketInfoDto> bucketInfos = new ArrayList<>();
        storageStateData.put("buckets", bucketInfos);
        List<Bucket> buckets = s3.listBuckets();
        long usedBytes = 0;
        long objectsCount = 0;
        for (Bucket bucket : buckets) {
            BucketInfoDto bucketInfoDto = new BucketInfoDto();
            bucketInfoDto.setCreated(bucket.getCreationDate().toInstant());
            bucketInfoDto.setName(bucket.getName());
            List<S3ObjectSummary> objectSummarries = s3.listObjects(bucket.getName()).getObjectSummaries();
            long usedSpace = objectSummarries.isEmpty() ? 0 : objectSummarries.stream().map(S3ObjectSummary::getSize).reduce((fst, snd) -> fst + snd).get();
            usedBytes += usedSpace;
            objectsCount += objectSummarries.size();
            bucketInfoDto.setUsedBytes(usedSpace);
            bucketInfoDto.setObjectsCount(objectSummarries.size());
            s3.getBucketAcl(bucket.getName())
                    .getGrantsAsList()
                    .stream()
                    .filter(g -> g.getGrantee().getIdentifier().equals(s3AccountOwner.getId()))
                    .forEach(g -> bucketInfoDto.addPermission(g.getPermission().toString()));
            bucketInfos.add(bucketInfoDto);
        }
        storageStateData.put("usedBytes", usedBytes);
        storageStateData.put("objectsCount", objectsCount);
        if (sshPort != 0) {
            try (SSHClient ssh = new SSHClient()) {
                sshConnect(ssh);
                storageStateData.put("cmd: " + CMD_STATUS, fetchDataFromRemote(ssh, createCmd(CMD_STATUS), storage));
                List<String> cephDfResult = fetchDataFromRemote(ssh, createCmd(CMD_DF), storage);
                storageStateData.put("cmd: " + CMD_DF, cephDfResult);
                Pattern regex = Pattern.compile("\\s*(\\w+)\\.rgw\\.buckets\\.data.+");
                Map<String, List<String>> poolDetailMap = new HashMap<>();
                for (String s : cephDfResult) {
                    Matcher matcher = regex.matcher(s);
                    if (matcher.find()) {
                        String poolName = matcher.group(1);
                        poolDetailMap.put(poolName, fetchDataFromRemote(ssh, createCmd(CMD_PGS) + " " + poolName + ".rgw.buckets.data", storage));
                    }
                }
                storageStateData.put("cmd: " + CMD_PGS, poolDetailMap);
            } catch (IOException e) {
                throw new SshException(e, storage);
            }
        }
        return new StorageStateDto(getStorage(), storageStateData);
    }

    @Setter
    @Getter
    public class BucketInfoDto {
        private String name;
        private Instant created;
        private long usedBytes;
        private long objectsCount;
        private List<String> accountPermissions = new ArrayList<>();

        public void addPermission(String permission) {
            accountPermissions.add(permission);
        }
    }

    @Override
    public boolean testConnection() {
        try {
            AmazonS3 s3 = connect();
            s3.getS3AccountOwner();
        } catch (Exception e) {
            log.error(storage.getName() + " unable to connect: " + e.getClass() + " " + e.getMessage());
            return false;
        }
        return true;
    }

    @Override
    public void createNewDataSpace(String dataSpace) {
        AmazonS3 s3 = connect();
        s3.createBucket(dataSpace);
    }

    @Override
    public ArchivalObjectDto verifyStateOfObjects(List<ArchivalObjectDto> objects, AtomicLong counter) throws StorageException {
        AmazonS3 s3 = connect();
        for (ArchivalObjectDto inputObject : objects) {
            if (!inputObject.getState().metadataMustBeStoredAtLogicalStorage()) {
                counter.incrementAndGet();
                continue;
            }
            String dataspace = inputObject.getOwner().getDataSpace();
            boolean objectMetadataExists = s3.doesObjectExist(dataspace, toMetadataObjectId(inputObject.getStorageId()));
            if (objectMetadataExists) {
                ObjectMetadata objectMetadata = s3.getObjectMetadata(dataspace, toMetadataObjectId(inputObject.getStorageId()));
                Map<String, String> userMetadata = objectMetadata.getUserMetadata();
                String stateAtStorage = userMetadata.get(STATE_KEY);
                if (inputObject.getState().toString().equals(stateAtStorage)) {
                    counter.incrementAndGet();
                    continue;
                }
            }
            return inputObject;
        }
        return null;
    }

    @Override
    public List<ArchivalObjectDto> createDtosForAllObjects(String dataSpace) {
        throw new UnsupportedOperationException();
    }

    private ObjectConsistencyVerificationResultDto fillObjectStateInfo(AmazonS3 s3, ObjectConsistencyVerificationResultDto info, ArchivalObjectDto object, String dataSpace) throws FileDoesNotExistException {
        info.setStorageId(object.getStorageId());
        info.setState(object.getState());
        info.setDatabaseChecksum(object.getChecksum());
        info.setDatabaseId(object.getDatabaseId());
        info.setCreated(object.getCreated());
        if (!object.getState().metadataMustBeStoredAtLogicalStorage()) {
            return info;
        }
        String metadataId = toMetadataObjectId(object.getStorageId());

        if (!s3.doesObjectExist(dataSpace, metadataId)) {
            info.setMetadataConsistent(false);
        } else {
            ObjectMetadata s3ObjectMetadata = s3.getObjectMetadata(dataSpace, metadataId);
            Map<String, String> userMetadata = s3ObjectMetadata.getUserMetadata();
            boolean stateMetadataConsistent = object.getState().toString().equals(userMetadata.get(STATE_KEY));
            String storageChecksumMetadataValue = userMetadata.get(object.getChecksum().getType().toString());
            boolean checksumMetadataConsistent = object.getChecksum().getValue().equals(storageChecksumMetadataValue);
            boolean timestampMetadataConsistent = object.getCreated().getEpochSecond() == Long.parseLong(userMetadata.get(CREATED_KEY));
            info.setMetadataConsistent(stateMetadataConsistent && checksumMetadataConsistent && timestampMetadataConsistent);
        }
        if (object.getState().contentMustBeStoredAtLogicalStorage()) {
            checkFileExists(s3, object.getStorageId(), dataSpace);
            S3Object s3Object = s3.getObject(dataSpace, object.getStorageId());
            Checksum storageFileChecksum = StorageUtils.computeChecksum(s3Object.getObjectContent(),
                    object.getChecksum().getType());
            info.setStorageChecksum(storageFileChecksum);
            if (info.getDatabaseChecksum().equals(storageFileChecksum))
                info.setContentConsistent(true);
        }
        return info;
    }

    private String createCmd(@NonNull String cmd) {
        StringBuilder strTmp = new StringBuilder(0);
        strTmp.append("sudo ");
        if (!cephBinHome.isEmpty()) {
            if (!cephBinHome.endsWith(System.getProperty("file.separator"))) {
                strTmp.append(cephBinHome).append(System.getProperty("file.separator")).append(cmd);
            } else {
                strTmp.append(cephBinHome).append(cmd);
            }
        } else {
            strTmp.append(cmd);
        }
        if (!this.cluster.isEmpty()) {
            strTmp.append(" --cluster ").append(cluster);
        }
        return strTmp.toString();
    }

    /**
     * Stores file and then reads it and verifies its fixity.
     * <p>
     * If rollback is set to true by another thread, this method returns ASAP (without throwing exception), leaving the file uncompleted but closing stream.  Uncompleted files are to be cleaned during rollback.
     * </p>
     * <p>
     * In case of any exception, rollback flag is set to true.
     * </p>
     */
    void storeFile(AmazonS3 s3, String id, InputStream stream, Checksum checksum, AtomicBoolean rollback, String dataSpace, Instant timestamp) throws FileCorruptedAfterStoreException, IOStorageException {
        if (rollback.get())
            return;
        try (BufferedInputStream bis = new BufferedInputStream(stream)) {
            InitiateMultipartUploadRequest initReq = new InitiateMultipartUploadRequest(dataSpace, id, new ObjectMetadata());
            InitiateMultipartUploadResult initRes = s3.initiateMultipartUpload(initReq);

            PutObjectRequest metadataPutRequest = storeMetadata(s3, id, checksum, ObjectState.PROCESSING, dataSpace, timestamp);

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
                        .withBucketName(dataSpace)
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
                if (!partChecksum.getValue().equalsIgnoreCase(uploadPartResult.getETag()))
                    throw new FileCorruptedAfterStoreException("S3 - part of multipart file", new Checksum(ChecksumType.MD5, uploadPartResult.getETag()), partChecksum, storage);
                partETags.add(uploadPartResult.getPartETag());
            } while (!last);
            CompleteMultipartUploadRequest completeReq = new CompleteMultipartUploadRequest(dataSpace, id, initRes.getUploadId(), partETags);
            s3.completeMultipartUpload(completeReq);
            metadataPutRequest.getMetadata().addUserMetadata(STATE_KEY, ObjectState.ARCHIVED.toString());
            s3.putObject(metadataPutRequest);
        } catch (Exception e) {
            rollback.set(true);
            if (e instanceof IOException)
                throw new IOStorageException(e, storage);
            if (e instanceof FileCorruptedAfterStoreException)
                throw (FileCorruptedAfterStoreException) e;
            throw new GeneralException(e);
        }
    }

    void rollbackFile(AmazonS3 s3, ArchivalObjectDto dto, String dataSpace) {
        String id = dto.getStorageId();
        storeMetadata(s3, id, dto.getChecksum(), ObjectState.ROLLED_BACK, dataSpace, dto.getCreated());
        List<MultipartUpload> multipartUploads = s3.listMultipartUploads(new ListMultipartUploadsRequest(dataSpace).withPrefix(id)).getMultipartUploads();
        if (multipartUploads.size() == 1)
            s3.abortMultipartUpload(new AbortMultipartUploadRequest(dataSpace, id, multipartUploads.get(0).getUploadId()));
        else if (multipartUploads.size() > 1)
            throw new GeneralException("unexpected error during rollback of file: " + id + " : there are more than one upload in progress");
        s3.deleteObject(dataSpace, id);
    }

    AmazonS3 connect() {
        AWSCredentials credentials = new BasicAWSCredentials(userAccessKey, userSecretKey);
        AWSStaticCredentialsProvider provider = new AWSStaticCredentialsProvider(credentials);
        ClientConfiguration clientConfig = new ClientConfiguration();
        //force usage of AWS signature v2 instead of v4 to enable multipart uploads (v4 does not work with multipart upload for now)
        //update: signature v4 should work with multipart in ceph mimic
        //clientConfig.setSignerOverride("S3SignerType");
        if (https)
            clientConfig.setProtocol(Protocol.HTTPS);
        else
            clientConfig.setProtocol(Protocol.HTTP);
        clientConfig.setConnectionTimeout(connectionTimeout);
        AmazonS3 conn = AmazonS3ClientBuilder
                .standard()
                .withCredentials(provider)
                .withClientConfiguration(clientConfig)
                .withPathStyleAccessEnabled(!virtualHost)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(storage.getHost() + ":" + storage.getPort(), region))
                .build();
        return conn;
    }

    private PutObjectRequest storeMetadata(AmazonS3 s3, String objId, Checksum checksum, ObjectState state, String dataSpace, Instant created) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.addUserMetadata(checksum.getType().toString(), checksum.getValue());
        objectMetadata.addUserMetadata(STATE_KEY, state.toString());
        objectMetadata.addUserMetadata(CREATED_KEY, Long.toString(created.getEpochSecond()));
        objectMetadata.setContentLength(0);
        PutObjectRequest metadataPutRequest = new PutObjectRequest(dataSpace, toMetadataObjectId(objId), new NullInputStream(0), objectMetadata);
        s3.putObject(metadataPutRequest);
        return metadataPutRequest;
    }

    private void checkFileExists(AmazonS3 s3, String id, String dataSpace) throws FileDoesNotExistException {
        boolean exist = s3.doesObjectExist(dataSpace, id);
        if (!exist)
            throw new FileDoesNotExistException("bucket: " + dataSpace + " storageId: " + id, storage);
    }

    private void sshConnect(SSHClient ssh) throws IOException {
        ssh.addHostKeyVerifier(new PromiscuousVerifier());
        ssh.setConnectTimeout(connectionTimeout);
        ssh.connect(sshServer, sshPort);
        ssh.authPublickey(sshUserName, sshKeyFilePath);
    }

    String toMetadataObjectId(String objId) {
        return objId + ".meta";
    }

    private class ClosableS3 implements Closeable {

        private AmazonS3 s3;

        public ClosableS3(AmazonS3 s3) {
            this.s3 = s3;
        }

        @Override
        public void close() throws IOException {
            s3.shutdown();
        }
    }
}
