package cz.cas.lib.arcstorage.jms;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class JmsHealthCheckListener {

    @JmsListener(destination = JmsConstants.HEALTHCHECK_QUEUE)
    public boolean handle() {
        return true;
    }
}
