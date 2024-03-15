package cz.cas.lib.arcstorage.dto;

import cz.cas.lib.arcstorage.storagesync.AuditedOperation;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ObjectAuditDto {
    private String id;
    private String idInDatabase;
    private String idInStorage;
    private Instant created;
    private String dataSpace;
    private AuditedOperation operation;
}
