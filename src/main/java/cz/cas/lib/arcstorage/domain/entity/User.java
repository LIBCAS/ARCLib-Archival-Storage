package cz.cas.lib.arcstorage.domain.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import cz.cas.lib.arcstorage.security.Role;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.BatchSize;

import javax.persistence.*;
import javax.validation.constraints.NotNull;

/**
 * User
 */
@Getter
@Setter
@BatchSize(size = 100)
@Entity
@Table(name = "arcstorage_user")
@NoArgsConstructor
public class User extends DatedObject {

    public User(String id, String username, String password, String dataSpace, Role role, String email) {
        this.id = id;
        this.username = username;
        this.password = password;
        this.dataSpace = dataSpace;
        this.role = role;
        this.email = email;
    }

    /**
     * Logging name
     */
    @NotNull
    private String username;

    /**
     * Password
     */
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private String password;

    private String email;

    /**
     * name of folder/bucket which will be created to store data
     * dataSpace should be simple string, no dashes, slashes, uppercase letters, spaces or underscores
     * should be null for ADMIN users
     */
    private String dataSpace;

    @NotNull
    @Column(name = "user_role")
    @Enumerated(EnumType.STRING)
    private Role role;

    public User(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "User{" +
                "id='" + id + '\'' +
                ", username='" + username + '\'' +
                ", email='" + email + '\'' +
                ", dataSpace='" + dataSpace + '\'' +
                '}';
    }
}
