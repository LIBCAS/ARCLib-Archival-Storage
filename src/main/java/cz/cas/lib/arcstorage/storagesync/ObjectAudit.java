package cz.cas.lib.arcstorage.storagesync;

import cz.cas.lib.arcstorage.domain.entity.DomainObject;
import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.domain.store.InstantGenerator;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;

import javax.persistence.*;
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
    private String objectId;
    @Column(updatable = false)
    @GeneratorType(type = InstantGenerator.class, when = GenerationTime.INSERT)
    private Instant created;
    @ManyToOne
    private User user;
    @Enumerated(EnumType.STRING)
    private ObjectType objectType;
    @Enumerated(EnumType.STRING)
    private AuditedOperation operation;

    public ObjectAudit(String objectId, User user, ObjectType objectType, AuditedOperation operation) {
        this.objectId = objectId;
        this.user = user;
        this.objectType = objectType;
        this.operation = operation;
    }

    @Override
    public String toString() {
        return "ObjectAudit{" +
                "objectId='" + objectId + '\'' +
                ", id='" + id + '\'' +
                ", created=" + created +
                ", user=" + user +
                ", objectType=" + objectType +
                ", operation=" + operation +
                '}';
    }
}
