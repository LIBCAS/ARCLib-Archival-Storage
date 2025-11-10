package cz.cas.lib.arcstorage.jms;

import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ObjectState;

import java.io.InputStream;
import java.time.Instant;

public class JmsArchivalObjectDto {
    private String storageId;
    private String databaseId;
    private Checksum checksum;
    /**
     * object may be populated just with id
     */
    private User owner;
    private InputStream inputStream;
    private ObjectState state;
    private Instant created;
    private ObjectType objectType;
}
