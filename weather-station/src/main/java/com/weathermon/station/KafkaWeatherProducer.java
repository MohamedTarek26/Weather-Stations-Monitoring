package com.weathermon.station;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Wraps Kafka ProducerAPI to send weather status messages.
 * Messages are keyed by station_id to guarantee per-station ordering.
 */
public class KafkaWeatherProducer implements AutoCloseable {

    private static final String DEFAULT_TOPIC = "weather-status";

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaWeatherProducer(String bootstrapServers) {
        this(bootstrapServers, DEFAULT_TOPIC);
    }

    public KafkaWeatherProducer(String bootstrapServers, String topic) {
        this.topic = topic;

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Ensure messages are durable
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        // Small batching for low-latency (1-second interval)
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "100");

        this.producer = new KafkaProducer<>(properties);
    }

    /**
     * Sends a weather status JSON message to Kafka.
     * 
     * @param stationId   used as the message key for partition locality
     * @param jsonMessage the full JSON weather status
     */
    public void send(long stationId, String jsonMessage) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(stationId), jsonMessage);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.printf("[Station %d] Failed to send: %s%n", stationId, exception.getMessage());
            } else {
                System.out.printf("[Station %d] Sent to %s partition %d offset %d%n",
                        stationId, metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
