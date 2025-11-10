package cz.cas.lib.arcstorage.dto;

import cz.cas.lib.arcstorage.jms.ArchivalObjectJmsDto;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.Instant;

@Getter
@AllArgsConstructor
public class StorageQueueDto<T extends ArchivalObjectJmsDto> {
    private final T data;
    private final Instant operationTimestamp;
    private final String userId;
}
