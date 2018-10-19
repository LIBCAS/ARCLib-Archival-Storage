package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.domain.entity.Configuration;
import cz.cas.lib.arcstorage.domain.store.ConfigurationStore;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.security.Roles;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.validation.Valid;

@Slf4j
@RestController
@RequestMapping("/api/administration")
@RolesAllowed(Roles.ADMIN)
public class SystemAdministrationApi {

    private ConfigurationStore configurationStore;

    @ApiOperation(value = "Creates/updates configuration of the Archival Storage", response = Configuration.class)
    @Transactional
    @RequestMapping(value = "/config", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "successful response"),
            @ApiResponse(code = 404, message = "config with the id is missing"),
            @ApiResponse(code = 400, message = "the config breaks the basic LTP policy (e.g. count of storages)"),
            @ApiResponse(code = 409, message = "provided config has different id than that stored in DB (only one config object is allowed)")
    })
    public Configuration save(
            @ApiParam(value = "configuration object", required = true) @RequestBody @Valid Configuration configuration
    ) {
        log.info("Saving new or updating an existing configuration of the Archival Storage.");
        return configurationStore.save(configuration);
    }

    @ApiOperation(value = "Returns configuration", response = Configuration.class)
    @RequestMapping(value = "/config", method = RequestMethod.GET)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "successful response"),
            @ApiResponse(code = 404, message = "config missing")
    })
    public Configuration get() {
        return configurationStore.get();
    }

    @Inject
    public void setConfigurationStore(ConfigurationStore configurationStore) {
        this.configurationStore = configurationStore;
    }
}
