package cz.cas.lib.arcstorage.storagesync;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.DomainObject;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.domain.store.InstantGenerator;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;

import jakarta.persistence.*;
import java.time.Instant;

/**
 * entity for auditing of operations on a object, currently used only for storagesync purposes - only delete, renew and removeObject operations are created
 */
@Entity
@Table(name = "arcstorage_object_audit")
@NoArgsConstructor
@Getter
public class ObjectAudit extends DomainObject {
    /**
     * the UUID of the object in database
     */
    private String idInDatabase;
    /**
     * may be null if it is equal to idInDatabase (only XML has different idInStorage)
     */
    private String idInStorage;

    @Column(updatable = false)
    @GeneratorType(type = InstantGenerator.class, when = GenerationTime.INSERT)
    private Instant created;

    @ManyToOne
    private User user;

    @Enumerated(EnumType.STRING)
    private AuditedOperation operation;

    public ObjectAudit(ArchivalObject obj, User user, AuditedOperation operation) {
        this(obj.toDto(), user, operation);
    }

    public ObjectAudit(ArchivalObjectDto dto, User user, AuditedOperation operation) {
        this.user = user;
        this.operation = operation;
        this.idInDatabase = dto.getDatabaseId();
        if (!dto.getDatabaseId().equals(dto.getStorageId()))
            this.idInStorage = dto.getStorageId();
    }

    public String getIdInStorage() {
        return idInStorage == null ? idInDatabase : idInStorage;
    }

    @Override
    public String toString() {
        return "ObjectAudit{" +
                "id='" + id + '\'' +
                ", idInDatabase='" + idInDatabase + '\'' +
                ", idInStorage='" + idInStorage + '\'' +
                ", created=" + created +
                ", user=" + user +
                ", operation=" + operation +
                '}';
    }
}
