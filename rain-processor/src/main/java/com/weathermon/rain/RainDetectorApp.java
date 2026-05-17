package com.weathermon.rain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Rain Detector — Kafka Streams Application
 *
 * Reads from the "weather-status" topic, filters messages where humidity > 70%,
 * and produces rain alert messages to the "rain-alerts" topic.
 *
 * Uses the Kafka Streams DSL (high-level API) for simplicity.
 *
 * Configuration via environment variables:
 *   KAFKA_BOOTSTRAP  - Kafka bootstrap servers (default: localhost:9092)
 *   INPUT_TOPIC      - Source topic (default: weather-status)
 *   OUTPUT_TOPIC     - Alerts topic (default: rain-alerts)
 *   HUMIDITY_THRESHOLD - Humidity % to trigger (default: 70)
 */
public class RainDetectorApp {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        // Configuration
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        String inputTopic = System.getenv().getOrDefault("INPUT_TOPIC", "weather-status");
        String outputTopic = System.getenv().getOrDefault("OUTPUT_TOPIC", "rain-alerts");
        int humidityThreshold = Integer.parseInt(
                System.getenv().getOrDefault("HUMIDITY_THRESHOLD", "70"));

        System.out.println("=== Rain Detector starting ===");
        System.out.printf("Kafka: %s%n", bootstrap);
        System.out.printf("Input: %s → Output: %s%n", inputTopic, outputTopic);
        System.out.printf("Threshold: humidity > %d%%%n", humidityThreshold);
        System.out.println("==============================");

        // Kafka Streams configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rain-detector");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Build the stream topology
        StreamsBuilder builder = new StreamsBuilder();

        // Read from weather-status topic
        KStream<String, String> weatherStream = builder.stream(inputTopic);

        // Filter: only keep messages where humidity > threshold
        KStream<String, String> rainAlerts = weatherStream
                .filter((key, value) -> {
                    try {
                        JsonNode root = mapper.readTree(value);
                        int humidity = root.path("weather").path("humidity").asInt(0);
                        return humidity > humidityThreshold;
                    } catch (Exception e) {
                        System.err.println("Failed to parse message: " + e.getMessage());
                        return false;
                    }
                })
                // Transform into a rain alert message
                .mapValues((key, value) -> {
                    try {
                        JsonNode original = mapper.readTree(value);

                        ObjectNode alert = mapper.createObjectNode();
                        alert.put("station_id", original.path("station_id").asLong());
                        alert.put("s_no", original.path("s_no").asLong());
                        alert.put("humidity", original.path("weather").path("humidity").asInt());
                        alert.put("alert_timestamp", System.currentTimeMillis() / 1000);
                        alert.put("message", String.format(
                                "Rain detected at station %d (humidity: %d%%)",
                                original.path("station_id").asLong(),
                                original.path("weather").path("humidity").asInt()));

                        return mapper.writeValueAsString(alert);
                    } catch (Exception e) {
                        System.err.println("Failed to create alert: " + e.getMessage());
                        return value; // pass through original on error
                    }
                });

        // Write alerts to rain-alerts topic
        rainAlerts.peek((key, value) -> {
            System.out.printf("[RAIN ALERT] %s%n", value);
        }).to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        // Build and start
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Graceful shutdown
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Rain Detector...");
            streams.close();
            latch.countDown();
        }));

        try {
            streams.start();
            latch.await(); // block until shutdown
        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
