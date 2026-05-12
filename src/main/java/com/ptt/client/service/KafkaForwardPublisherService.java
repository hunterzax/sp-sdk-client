package com.ptt.client.service;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Properties;

/**
 * Optional second Kafka producer: republishes consumed payloads (raw JSON) to a separate cluster
 * (e.g. Redpanda) with its own bootstrap, SASL, and topic. Independent from the Datalink consumer settings.
 */
@Service
@ConditionalOnProperty(name = "kafka.forward.enabled", havingValue = "true")
public class KafkaForwardPublisherService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaForwardPublisherService.class);

    @Value("${kafka.forward.bootstrap.servers:}")
    private String bootstrapServers;

    @Value("${kafka.forward.topic:}")
    private String topic;

    @Value("${kafka.forward.client.id:}")
    private String clientId;

    @Value("${kafka.forward.username:}")
    private String username;

    @Value("${kafka.forward.password:}")
    private String password;

    @Value("${kafka.forward.ssl.enabled:false}")
    private boolean sslEnabled;

    @Value("${kafka.forward.sasl.mechanism:}")
    private String saslMechanism;

    private KafkaProducer<String, String> producer;

    @PostConstruct
    public void startProducer() {
        validateForwardEnv();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.trim());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId.trim());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");

        applySecurity(props);

        producer = new KafkaProducer<>(props);
        logger.info("Kafka forward producer started: clientId={}, topic={}, bootstrap={}", clientId.trim(), topic,
                bootstrapServers);
    }

    private void validateForwardEnv() {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalStateException(
                    "[Kafka forward] KAFKA_FORWARD_ENABLED=true but KAFKA_FORWARD_BROKERS is missing or empty");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalStateException(
                    "[Kafka forward] KAFKA_FORWARD_ENABLED=true but KAFKA_FORWARD_TOPIC is missing or empty");
        }
        if (clientId == null || clientId.isBlank()) {
            throw new IllegalStateException(
                    "[Kafka forward] KAFKA_FORWARD_ENABLED=true but KAFKA_FORWARD_CLIENT_ID is missing or empty");
        }

        String mechanism = saslMechanism == null ? "" : saslMechanism.trim();
        String user = username == null ? "" : username.trim();
        if (!mechanism.isEmpty() && user.isEmpty()) {
            throw new IllegalStateException(
                    "[Kafka forward] KAFKA_FORWARD_SASL_MECHANISM is set (" + mechanism
                            + ") but KAFKA_FORWARD_USERNAME is missing or empty");
        }
        if (mechanism.isEmpty() && !user.isEmpty()) {
            throw new IllegalStateException(
                    "[Kafka forward] KAFKA_FORWARD_USERNAME is set but KAFKA_FORWARD_SASL_MECHANISM is missing or empty "
                            + "(set e.g. SCRAM-SHA-256, SCRAM-SHA-512, or PLAIN)");
        }
    }

    private void applySecurity(Properties props) {
        String mechanism = saslMechanism == null ? "" : saslMechanism.trim();
        if (!mechanism.isEmpty()) {
            if (username == null || username.isBlank()) {
                throw new IllegalStateException(
                        "[Kafka forward] KAFKA_FORWARD_SASL_MECHANISM is set but KAFKA_FORWARD_USERNAME is empty");
            }
            String protocol = sslEnabled ? "SASL_SSL" : "SASL_PLAINTEXT";
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);
            props.put(SaslConfigs.SASL_MECHANISM, mechanism);
            props.put(SaslConfigs.SASL_JAAS_CONFIG, buildJaas(username, password == null ? "" : password, mechanism));
            if (sslEnabled) {
                props.put("ssl.endpoint.identification.algorithm", "");
            }
        } else if (sslEnabled) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            props.put("ssl.endpoint.identification.algorithm", "");
        } else {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        }
    }

    private static String buildJaas(String user, String pass, String mechanism) {
        String u = escapeJaas(user);
        String p = escapeJaas(pass);
        if ("SCRAM-SHA-256".equalsIgnoreCase(mechanism) || "SCRAM-SHA-512".equalsIgnoreCase(mechanism)) {
            return "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + u + "\" password=\""
                    + p + "\";";
        }
        if ("PLAIN".equalsIgnoreCase(mechanism)) {
            return "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + u
                    + "\" password=\"" + p + "\";";
        }
        throw new IllegalStateException("[Kafka forward] Unsupported KAFKA_FORWARD_SASL_MECHANISM: " + mechanism
                + " (supported: SCRAM-SHA-256, SCRAM-SHA-512, PLAIN)");
    }

    private static String escapeJaas(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Publishes the same JSON string as received from the Datalink consumer.
     */
    public void publish(String rawJson) {
        if (producer == null) {
            return;
        }
        try {
            producer.send(new ProducerRecord<>(topic.trim(), null, rawJson), (metadata, exception) -> {
                if (exception != null) {
                    logger.warn("Kafka forward send failed: {}", exception.getMessage());
                }
            });
        } catch (Exception e) {
            logger.error("Kafka forward publish error", e);
        }
    }

    @PreDestroy
    public void close() {
        if (producer != null) {
            producer.close();
            logger.info("Kafka forward producer closed");
        }
    }
}
