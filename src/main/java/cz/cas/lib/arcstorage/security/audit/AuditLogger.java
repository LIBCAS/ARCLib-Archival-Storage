package cz.cas.lib.arcstorage.security.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Audit logger.
 * <p>
 * Logs audit events, possibly to separate file or database for latter inspection.
 */
@Service
public class AuditLogger {
    private final Logger log = LoggerFactory.getLogger("Audit");

    private static final Marker MARKER = MarkerFactory.getMarker("AUDIT");

    private ObjectMapper mapper;

    /**
     * Logs the event.
     *
     * @param event {@link AuditEvent} to log
     */
    public void logEvent(AuditEvent event) {
        try {
            String message = mapper.writer().writeValueAsString(event);
            log.debug(MARKER, message);

        } catch (JsonProcessingException e) {
            log.error("Failed to log audit event.", e);
        }
    }

    @Autowired
    public void setMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }
}
