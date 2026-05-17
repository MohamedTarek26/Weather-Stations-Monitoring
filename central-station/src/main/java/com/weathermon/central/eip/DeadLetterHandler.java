package com.weathermon.central.eip;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * EIP Pattern: Dead Letter Channel
 *
 * Messages that fail processing (BitCask write error, Parquet failure, etc.)
 * after the maximum number of retries are routed to a dedicated "weather-dlq"
 * Kafka topic. This prevents message loss while isolating problematic messages.
 */
public class DeadLetterHandler implements AutoCloseable {

    private final KafkaProducer<String, String> producer;
    private final String dlqTopic;
    private final int maxRetries;

    public DeadLetterHandler(String bootstrap, String dlqTopic, int maxRetries) {
        this.dlqTopic = dlqTopic;
        this.maxRetries = maxRetries;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "dead-letter-handler");

        this.producer = new KafkaProducer<>(props);
        System.out.printf("[EIP] Dead Letter Channel → topic '%s' (max retries: %d)%n",
                dlqTopic, maxRetries);
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Routes a failed message to the dead letter topic after retries are exhausted.
     *
     * @param key     original Kafka key
     * @param value   the message that failed processing
     * @param error   the exception that caused the failure
     * @param attempt the attempt number that finally failed
     */
    public void route(String key, String value, Exception error, int attempt) {
        String envelope = String.format(
                "{\"original_key\":\"%s\",\"original_value\":%s,\"error\":\"%s\",\"attempts\":%d,\"timestamp\":%d}",
                key != null ? key : "null",
                value != null ? value : "null",
                error.getMessage() != null ? error.getMessage().replace("\"", "\\\"") : "unknown",
                attempt,
                System.currentTimeMillis());

        producer.send(new ProducerRecord<>(dlqTopic, key, envelope));
        System.err.printf("[EIP-DLQ] Routed to '%s': key=%s after %d attempts: %s%n",
                dlqTopic, key, attempt, error.getMessage());
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
