package cz.cas.lib.arcstorage.domain.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.BatchSize;

import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * User
 */
@Getter
@Setter
@BatchSize(size = 100)
@Entity
@Table(name = "arcstorage_user")
@AllArgsConstructor
@NoArgsConstructor
public class User extends DatedObject {
    /**
     * Logging name
     */
    private String username;

    /**
     * Password
     */
    @JsonIgnore
    private String password;
}
