package com.weathermon.central.eip;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * EIP Pattern: Invalid Message Channel
 *
 * Messages that cannot be parsed (malformed JSON, missing fields) are routed
 * to a dedicated "weather-invalid" Kafka topic instead of being silently
 * dropped.
 * This allows operators to inspect and reprocess failed messages.
 */
public class InvalidMessageHandler implements AutoCloseable {

    private final KafkaProducer<String, String> producer;
    private final String invalidTopic;

    public InvalidMessageHandler(String bootstrap, String invalidTopic) {
        this.invalidTopic = invalidTopic;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "invalid-message-handler");

        this.producer = new KafkaProducer<>(props);
        System.out.printf("[EIP] Invalid Message Channel → topic '%s'%n", invalidTopic);
    }

    /**
     * Routes an invalid message to the invalid-message topic.
     *
     * @param key    original Kafka key
     * @param value  the malformed message
     * @param reason why the message was rejected
     */
    public void route(String key, String value, String reason) {
        String envelope = String.format(
                "{\"original_key\":\"%s\",\"original_value\":%s,\"reason\":\"%s\",\"timestamp\":%d}",
                key != null ? key : "null",
                value != null ? "\"" + value.replace("\"", "\\\"") + "\"" : "null",
                reason.replace("\"", "\\\""),
                System.currentTimeMillis());

        producer.send(new ProducerRecord<>(invalidTopic, key, envelope));
        System.err.printf("[EIP-Invalid] Routed to '%s': key=%s reason=%s%n",
                invalidTopic, key, reason);
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
