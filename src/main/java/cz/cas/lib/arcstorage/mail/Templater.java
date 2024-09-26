package cz.cas.lib.arcstorage.mail;


import freemarker.template.Configuration;
import freemarker.template.TemplateException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.ui.freemarker.FreeMarkerTemplateUtils;

import java.io.IOException;
import java.util.Map;

import static org.apache.commons.codec.CharEncoding.UTF_8;

@Service
public class Templater {
    private final Configuration freeMarkerConfig;

    @Autowired
    public Templater(Configuration freeMarkerConfig) {
        this.freeMarkerConfig = freeMarkerConfig;
        this.freeMarkerConfig.setClassLoaderForTemplateLoading(this.getClass().getClassLoader(), "/");
    }

    public String transform(String template, Map<String, Object> beans) throws TemplateException, IOException {
        return FreeMarkerTemplateUtils.processTemplateIntoString(
                freeMarkerConfig.getTemplate(template, UTF_8), beans);
    }
}