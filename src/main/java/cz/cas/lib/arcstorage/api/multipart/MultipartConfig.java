package cz.cas.lib.arcstorage.api.multipart;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MultipartConfig {
    @Bean
    public ArcstorageMultipartResolver multipartResolver() {
        return new ArcstorageMultipartResolver();
    }
}
