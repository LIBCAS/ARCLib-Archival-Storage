package cz.cas.lib.arcstorage.config;

import cz.cas.lib.arcstorage.jms.JmsConstants;
import jakarta.jms.ConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.region.policy.RedeliveryPolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jms.activemq.ActiveMQConnectionFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.MessageType;

@Configuration
public class JmsClientConfig {

    private String brokerUrl;

    @Bean
    public ConnectionFactory connectionFactory() {
        return new ActiveMQConnectionFactory(brokerUrl);
    }

    @Bean
    public MessageConverter jacksonJmsMessageConverter() {
        MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
        converter.setTargetType(MessageType.TEXT);
        converter.setTypeIdPropertyName("_type");
        return converter;
    }

    @Bean
    public ActiveMQConnectionFactoryCustomizer configureRedeliveryPolicy() {
        return connectionFactory ->
        {
            RedeliveryPolicyMap redeliveryPolicyMap = new RedeliveryPolicyMap();

            RedeliveryPolicy storagesQueuePolicy = new RedeliveryPolicy();
            storagesQueuePolicy.setMaximumRedeliveries(-1);
            //the interval does not really matters since the app is designed to detach consumer in case of error
            //the message is redelivered when the consumer is again attached
            storagesQueuePolicy.setRedeliveryDelay(60000);
            storagesQueuePolicy.setInitialRedeliveryDelay(60000);
            redeliveryPolicyMap.put(new ActiveMQQueue(JmsConstants.QUEUE_PREFIX + "*"), storagesQueuePolicy);

            RedeliveryPolicy healthcheckQueuePolicy = new RedeliveryPolicy();
            healthcheckQueuePolicy.setMaximumRedeliveries(0);
            redeliveryPolicyMap.put(new ActiveMQQueue(JmsConstants.HEALTHCHECK_QUEUE), healthcheckQueuePolicy);

            connectionFactory.setRedeliveryPolicyMap(redeliveryPolicyMap);
        };
    }

    @Autowired
    public void setBrokerUrl(@Value("${spring.activemq.broker-url:vm://localhost}") String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }
}
