package cz.cas.lib.arcstorage.storage;

public interface StorageServiceTest {

    void storeLargeFileSuccessTest() throws Exception;

    void storeSmallFileSuccessTest() throws Exception;

    void storeFileRollbackAware() throws Exception;

    void storeFileSettingRollback() throws Exception;

    void storeAipOk() throws Exception;

    void storeAipSetsRollback() throws Exception;

    void storeXmlOk() throws Exception;

    void getAipWithMoreXmlsOk() throws Exception;

    void getAipWithSpecificXmlOk() throws Exception;

    void getAipMissing() throws Exception;

    void getAipMissingXml() throws Exception;

    void getXmlOk() throws Exception;

    void getXmlMissing() throws Exception;

    void deleteSipMultipleTimesOk() throws Exception;

    void deleteSipMissingMetadata() throws Exception;

    void removeSipMultipleTimesOk() throws Exception;

    void removeSipMissingMetadata() throws Exception;

    void getAipInfoOk() throws Exception;

    void getAipInfoMissingSip() throws Exception;

    void getAipInfoMissingXml() throws Exception;

    void getAipInfoDeletedSip() throws Exception;

    void rollbackProcessingFile() throws Exception;

    void rollbackStoredFileMultipleTimes() throws Exception;

    void rollbackCompletlyMissingFile() throws Exception;

    void rollbackAipOk() throws Exception;

    void rollbackXmlOk() throws Exception;

    void testConnection() throws Exception;
}
