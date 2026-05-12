package com.ptt.client.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ptt.client.model.SensorDataRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.Collections;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service to consume data from Kafka and persist to database / broadcast via
 * WebSocket.
 */
@Service
public class KafkaSubscriberService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSubscriberService.class);

    private final DatabaseService databaseService;
    private final WebSocketService webSocketService;
    private final ObjectMapper objectMapper;
    private final ObjectProvider<MssqlWriterService> mssqlWriter;
    private final ObjectProvider<KafkaForwardPublisherService> forwardPublisher;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private ExecutorService executorService;
    private KafkaConsumer<String, String> consumer;

    @Value("${kafka.bootstrap.servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${kafka.topic:sensor-data}")
    private String topic;

    @Value("${kafka.group.id:client-app-group}")
    private String groupId;

    @Value("${client.live.source:kafka}")
    private String liveSource;

    @Autowired
    public KafkaSubscriberService(DatabaseService databaseService, WebSocketService webSocketService,
            ObjectMapper objectMapper, ObjectProvider<MssqlWriterService> mssqlWriter,
            ObjectProvider<KafkaForwardPublisherService> forwardPublisher) {
        this.databaseService = databaseService;
        this.webSocketService = webSocketService;
        this.objectMapper = objectMapper;
        this.mssqlWriter = mssqlWriter;
        this.forwardPublisher = forwardPublisher;
    }

    @PostConstruct
    public void startConsumer() {
        if (!isKafkaLiveSource()) {
            logger.info("Kafka consumer disabled (client.live.source={})", liveSource.trim());
            return;
        }
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));

        this.executorService = Executors.newSingleThreadExecutor();
        this.executorService.submit(this::consumeLoop);
    }

    private boolean isKafkaLiveSource() {
        String s = liveSource.trim().toLowerCase(Locale.ROOT);
        return "kafka".equals(s) || "both".equals(s);
    }

    private void consumeLoop() {
        logger.info("Kafka consumer loop started");
        try {
            while (running.get()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        String payload = record.value();

                        // Broadcast to WS (optional)
                        webSocketService.broadcast(payload);

                        // Persist to DB + optional MSSQL mirror + optional forward publish
                        try {
                            SensorDataRecord sensorData = objectMapper.readValue(payload, SensorDataRecord.class);
                            databaseService.insertRecord(sensorData);
                            mssqlWriter.ifAvailable(w -> w.write(sensorData));
                            forwardPublisher.ifAvailable(p -> p.publish(payload));
                        } catch (Exception e) {
                            logger.error("Failed to process record: {}", payload, e);
                        }
                    }
                } catch (Exception e) {
                    if (running.get()) {
                        logger.error("Error in consumption loop", e);
                    }
                }
            }
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            logger.info("Kafka consumer closed");
        }
    }

    @PreDestroy
    public void stopConsumer() {
        running.set(false);
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }
}
