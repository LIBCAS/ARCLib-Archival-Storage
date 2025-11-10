package cz.cas.lib.arcstorage.config;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.nio.file.Paths;

@Configuration
@ConditionalOnProperty(prefix = "spring.activemq", name = "broker-url", havingValue = "vm://localhost", matchIfMissing = true)
public class JmsBrokerConfig {

    private String kahaDbFolder;

    @Bean(initMethod = "start", destroyMethod = "stop")
    public BrokerService brokerService() throws Exception {
        BrokerService broker = new BrokerService();
        broker.addConnector("vm://localhost");
        PersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        File dataDir = Paths.get(kahaDbFolder).toFile();
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
        persistenceAdapter.setDirectory(dataDir);
        broker.setPersistenceAdapter(persistenceAdapter);
        broker.setDestinationPolicy(destinationPolicy());
        return broker;
    }

    public PolicyMap destinationPolicy() {
        PolicyMap policyMap = new PolicyMap();

        // Define a policy entry for queues
        PolicyEntry queuePolicy = new PolicyEntry();
        queuePolicy.setQueue(">");
        queuePolicy.setPrioritizedMessages(true); // Enable prioritization
        queuePolicy.setUseCache(false);           // Disable caching
        queuePolicy.setExpireMessagesPeriod(0);   // Disable expiration
        queuePolicy.setQueuePrefetch(1);          // Fetch one message at a time

        policyMap.put(queuePolicy.getDestination(), queuePolicy);
        return policyMap;
    }

    @Autowired
    public void setKahaDbFolder(@Value("${arcstorage.embedded-queue.data-dir:data/kahadb}") String kahaDbFolder) {
        this.kahaDbFolder = kahaDbFolder;
    }
}
