package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.exception.BadRequestException;
import cz.cas.lib.arcstorage.exception.ConflictObject;
import cz.cas.lib.arcstorage.security.Role;
import cz.cas.lib.arcstorage.security.Roles;
import cz.cas.lib.arcstorage.security.user.UserStore;
import cz.cas.lib.arcstorage.service.StorageProvider;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import io.swagger.annotations.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.validation.Valid;
import java.util.List;

import static cz.cas.lib.arcstorage.util.Utils.isNull;

@Slf4j
@RestController
@Api(value = "user", description = "managing arcstorage system users")
@RequestMapping("/api/user")
public class UserApi {

    private UserStore userStore;
    private PasswordEncoder passwordEncoder;
    private StorageProvider storageProvider;

    @ApiOperation(value = "Register user.", notes = "DataSpace should be simple string, no dashes, slashes, " +
            "uppercase letters, spaces or underscores. If the user has READ/READ-WRITE role and the dataSpace is new," +
            " the dataSpace should be activated by another endpoint.")
    @ApiResponses(value = {
            @ApiResponse(code = 409, message = "username already exists"),
            @ApiResponse(code = 400, message = "Wrong JSON | username/password/role missing | role!=ADMIN and dataSpace missing | " +
                    "role==ADMIN and dataSpace not null")
    })
    @RequestMapping(value = "/register", method = RequestMethod.POST)
    @Transactional
    @RolesAllowed(Roles.ADMIN)
    public User registerUser(@ApiParam(value = "User to be registered", required = true)
                             @RequestBody @Valid User user) throws BadRequestException {
        log.debug("Registering user with username " + user.getUsername() + ".");
        if (user.getRole() == Role.ROLE_ADMIN && user.getDataSpace() != null)
            throw new BadRequestException("admin user account in archival storage can't be bound to particular dataSpace," +
                    " dataSpace has to be null");
        else if (user.getRole() != Role.ROLE_ADMIN) {
            if (user.getDataSpace() == null)
                throw new BadRequestException("non-admin user account has to be bound with dataSpace");
            if (!user.getDataSpace().toLowerCase().equals(user.getDataSpace().toUpperCase()))
                throw new BadRequestException("dataSpace can't contain upperCase characters");
        }
        User existing = userStore.findByUsername(user.getUsername());
        isNull(existing, () -> new ConflictObject(existing));
        user.setPassword(passwordEncoder.encode(user.getPassword()));
        userStore.save(user);
        log.info("User with username " + user.getUsername() + " has been successfully registered.");
        return user;
    }

    @ApiOperation(value = "Activates dataSpace i.e. creates folders/buckets etc. at all logical storages.",
            notes = "DataSpace should be simple string, no dashes, slashes, uppercase letters, spaces or underscores.")
    @ApiResponses(value = {
            @ApiResponse(code = 503, message = "some logical storage not reachable or system is in readonly state"),
            @ApiResponse(code = 500, message = "no logical storage attached or internal server error"),
    })
    @RequestMapping(value = "/activate/{dataSpace}", method = RequestMethod.POST)
    @Transactional
    @RolesAllowed(Roles.ADMIN)
    public void activateDataSpace(
            @ApiParam(value = "dataSpace", required = true) @PathVariable("dataSpace") String dataSpace)
            throws NoLogicalStorageAttachedException, SomeLogicalStoragesNotReachableException, ReadOnlyStateException {
        List<StorageService> reachableAdapters = storageProvider.createAdaptersForWriteOperation();
        reachableAdapters.forEach(
                service -> {
                    try {
                        log.debug("Creating new data space: " + dataSpace + " at storage: " + service.getStorage().getName() + ".");
                        service.createNewDataSpace(dataSpace);
                        log.info("Date space: " + dataSpace + " successfully created at storage: " + service.getStorage().getName() + ".");
                    } catch (Exception e) {
                        throw new RuntimeException("unable to create dataSpace: " + e.getMessage(), e);
                    }
                }
        );
    }

    @Inject
    public void setStorageProvider(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }

    @Inject
    public void setUserStore(UserStore userStore) {
        this.userStore = userStore;
    }

    @Inject
    public void setPasswordEncoder(PasswordEncoder passwordEncoder) {
        this.passwordEncoder = passwordEncoder;
    }
}
