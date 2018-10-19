package cz.cas.lib.arcstorage.init;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import com.fasterxml.jackson.datatype.hibernate5.Hibernate5Module;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;

@Component
@Slf4j
public class PostInitializer implements ApplicationListener<ApplicationReadyEvent> {
    @Inject
    private DataSource ds;
    @Inject
    private ObjectMapper objecMapper;

    @Value("${env}")
    private String env;

    @Override
    public void onApplicationEvent(ApplicationReadyEvent events) {
        objecMapper.registerModule(new Hibernate5Module());
        objecMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objecMapper.setDateFormat(new ISO8601DateFormat());
        if (env == null)
            return;
        if (env.equals("staging")) {
            try {
                sqlTestInit();
                log.info("Data init successful");
            } catch (Exception e) {
                throw new RuntimeException("Data init error", e);
            }
        }
    }

    private void sqlTestInit() throws SQLException, IOException {
        try (Connection con = ds.getConnection()) {
            ScriptRunner runner = new ScriptRunner(con, false, true);
            runner.runScript(new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream("init.sql"))));
        }
    }
}

