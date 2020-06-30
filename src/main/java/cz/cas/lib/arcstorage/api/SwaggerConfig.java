package cz.cas.lib.arcstorage.api;

import io.swagger.annotations.ApiResponse;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.AuthorizationScopeBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.*;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spi.service.contexts.SecurityContext;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger.web.*;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.util.Collections;

@EnableSwagger2
@Configuration
public class SwaggerConfig {

    private static final String KEY_NAME = "Basic auth token";
    private static final String KEY = "Authorization";

    @Bean
    public Docket api() {
        return new Docket(DocumentationType.SWAGGER_2)
                .useDefaultResponseMessages(false)
                .select()
                .apis(RequestHandlerSelectors.basePackage("cz.cas.lib"))
                .paths(PathSelectors.any())
                .build()
                .securitySchemes(Collections.singletonList(apiKey()))
                .securityContexts(Collections.singletonList(securityContext()))
                .apiInfo(apiInfo());
    }

    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("Archival Storage Gateway API")
                .version("v1")
                .contact(new Contact("inQool, a.s.", "https://inqool.cz/", "info@inqool.cz"))
                .build();
    }

    private SecurityScheme apiKey() {
        return new ApiKey(KEY, KEY, ApiKeyVehicle.HEADER.getValue());
    }

    @Bean
    UiConfiguration uiConfig() {
        return UiConfigurationBuilder.builder()
                .docExpansion(DocExpansion.NONE)
                .operationsSorter(OperationsSorter.ALPHA)
                .defaultModelRendering(ModelRendering.EXAMPLE)
                .supportedSubmitMethods(UiConfiguration.Constants.DEFAULT_SUBMIT_METHODS)
                .showExtensions(true)
                .displayRequestDuration(true)
                .filter(Boolean.TRUE)
                .build();
    }

    private SecurityContext securityContext() {
        AuthorizationScope[] authScopes = new AuthorizationScope[1];
        authScopes[0] = new AuthorizationScopeBuilder().scope("global").description("full access").build();
        SecurityReference securityReference = SecurityReference.builder().reference(KEY)
                .scopes(authScopes).build();
        return SecurityContext.builder().securityReferences(Collections.singletonList(securityReference)).build();
    }


    /**
     * Possible values for {@link ApiResponse#responseContainer()} annotation property.
     */
    public interface ResponseContainer {

        String LIST = "List";
        String SET = "Set";
        String MAP = "Map";
    }
}
