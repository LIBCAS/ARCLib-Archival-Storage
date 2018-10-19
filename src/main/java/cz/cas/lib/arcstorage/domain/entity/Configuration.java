package cz.cas.lib.arcstorage.domain.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.BatchSize;

import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "arcstorage_configuration")
@NoArgsConstructor
@Getter
@Setter
@BatchSize(size = 100)
@AllArgsConstructor
public class Configuration extends DomainObject {
    private int minStorageCount;
    private boolean readOnly;
}
