package cz.cas.lib.arcstorage.jms.context;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.concurrent.ConcurrentMap;

@AllArgsConstructor
public class StorageContext {

    private String storageId;

    private boolean primary;

    /**
     * {@link cz.cas.lib.arcstorage.dto.ArchivalObjectDto#databaseId} is the key
     */
    @Getter
    private ConcurrentMap<String, ProcessingObjectContext> processingObjects;
}
