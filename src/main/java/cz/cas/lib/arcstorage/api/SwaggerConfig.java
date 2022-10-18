package cz.cas.lib.arcstorage.api;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.ExternalDocumentation;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class SwaggerConfig {
    @Bean
    public OpenAPI springShopOpenAPI() {
        return new OpenAPI()
                .security(List.of(new SecurityRequirement().addList("basicAuth")))
                .components(new Components().addSecuritySchemes("basicAuth", new SecurityScheme().type(
                        SecurityScheme.Type.HTTP).scheme("basic")))
                .info(new Info().title("Archival Storage Gateway API")
                        .version("v1.1")
                        .license(new License().name("GNU GPL v3"))
                        .contact(new Contact().name("inQool, a.s.").url("https://inqool.cz/").email("info@inqool.cz")))
                .externalDocs(new ExternalDocumentation()
                        .description("Archival Storage Documentation")
                        .url("https://frnk.lightcomp.cz/download/cuni-ais/doc/arcstorage/arcstorage.html"));
    }
}
