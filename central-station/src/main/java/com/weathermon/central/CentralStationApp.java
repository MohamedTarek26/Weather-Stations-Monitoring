package com.weathermon.central;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.weathermon.central.archiver.ParquetArchiver;
import com.weathermon.central.archiver.WeatherRecord;
import com.weathermon.central.bitcask.BitCaskStore;
import com.weathermon.central.api.BitCaskServer;
import com.weathermon.central.elastic.ElasticIndexer;
import com.weathermon.central.eip.InvalidMessageHandler;
import com.weathermon.central.eip.DeadLetterHandler;
import com.weathermon.central.eip.IdempotentReceiver;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Central Station Application — The heart of the system.
 *
 * Consumes weather status messages from Kafka, and for each message:
 * 1. Checks for duplicates (EIP: Idempotent Receiver)
 * 2. Validates JSON structure (EIP: Invalid Message Channel)
 * 3. Stores the latest status per station in BitCask (key = station_id)
 * 4. Archives every message to Parquet files (batched)
 * 5. Indexes to ElasticSearch (bulk, every 30s)
 * 6. On processing failure after retries → Dead Letter Channel
 *
 * EIP Patterns integrated:
 * - Idempotent Receiver: deduplicates by (station_id, s_no)
 * - Invalid Message Channel: bad JSON → "weather-invalid" topic
 * - Dead Letter Channel: processing failures → "weather-dlq" topic
 * - Envelope Wrapper: messages carry metadata envelope around weather payload
 *
 * Configuration via environment variables:
 * KAFKA_BOOTSTRAP - Kafka bootstrap servers (default: localhost:9092)
 * TOPIC - Kafka topic to consume (default: weather-status)
 * BITCASK_DIR - BitCask data directory (default: ./data/bitcask)
 * ARCHIVE_DIR - Parquet archive directory (default: ./data/archive)
 * API_PORT - HTTP API port (default: 8080)
 * ES_HOST - ElasticSearch host:port (default: elasticsearch-service:9200)
 * ES_INDEX - ElasticSearch index name (default: weather-status)
 */
public class CentralStationApp {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        // Read configuration
        String bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
        String topic = env("TOPIC", "weather-status");
        String bitcaskDir = env("BITCASK_DIR", "./data/bitcask");
        String archiveDir = env("ARCHIVE_DIR", "./data/archive");
        int apiPort = Integer.parseInt(env("API_PORT", "8080"));
        String esHost = env("ES_HOST", "elasticsearch-service:9200");
        String esIndex = env("ES_INDEX", "weather-status");

        System.out.println("=== Central Station starting ===");
        System.out.printf("Kafka: %s | Topic: %s%n", bootstrap, topic);
        System.out.printf("BitCask: %s | Archive: %s%n", bitcaskDir, archiveDir);
        System.out.printf("API port: %d%n", apiPort);
        System.out.println("================================");

        try {
            // Initialize storage
            BitCaskStore bitcask = new BitCaskStore(bitcaskDir);
            ParquetArchiver archiver = new ParquetArchiver(archiveDir);
            ElasticIndexer esIndexer = new ElasticIndexer(esHost, esIndex);

            // Initialize EIP handlers
            InvalidMessageHandler invalidHandler = new InvalidMessageHandler(bootstrap, "weather-invalid");
            DeadLetterHandler dlqHandler = new DeadLetterHandler(bootstrap, "weather-dlq", 3);
            IdempotentReceiver idempotentReceiver = new IdempotentReceiver(10000);

            // Start HTTP API server for BitCask client
            BitCaskServer apiServer = new BitCaskServer(bitcask, apiPort);
            apiServer.start();

            // Graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n[Central] Shutting down...");
                try {
                    archiver.flush();
                    esIndexer.close();
                    invalidHandler.close();
                    dlqHandler.close();
                    bitcask.close();
                    apiServer.stop();
                    System.out.printf("[Central] Duplicates skipped: %d%n",
                            idempotentReceiver.getDuplicateCount());
                } catch (Exception e) {
                    System.err.println("Error during shutdown: " + e.getMessage());
                }
            }));

            // Start consuming from Kafka
            consumeLoop(bootstrap, topic, bitcask, archiver, esIndexer,
                    invalidHandler, dlqHandler, idempotentReceiver);

        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Main Kafka consumer loop — runs forever, processing messages.
     */
    private static void consumeLoop(String bootstrap, String topic,
            BitCaskStore bitcask, ParquetArchiver archiver,
            ElasticIndexer esIndexer,
            InvalidMessageHandler invalidHandler,
            DeadLetterHandler dlqHandler,
            IdempotentReceiver idempotentReceiver) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-station");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.printf("[Central] Subscribed to topic: %s%n", topic);

            long messageCount = 0;

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    // ── EIP: Invalid Message Channel ──────────────
                    // Validate JSON before processing
                    JsonNode root;
                    try {
                        root = mapper.readTree(record.value());
                        if (root == null || !root.has("station_id") || !root.has("s_no")) {
                            invalidHandler.route(record.key(), record.value(),
                                    "Missing required fields: station_id or s_no");
                            continue;
                        }
                    } catch (Exception e) {
                        invalidHandler.route(record.key(), record.value(),
                                "JSON parse error: " + e.getMessage());
                        continue;
                    }

                    long stationId = root.path("station_id").asLong();
                    long sNo = root.path("s_no").asLong();

                    // ── EIP: Idempotent Receiver ──────────────────
                    // Skip duplicates based on (station_id, s_no)
                    if (idempotentReceiver.isDuplicate(stationId, sNo)) {
                        continue;
                    }

                    // ── EIP: Dead Letter Channel ──────────────────
                    // Retry processing up to maxRetries, then route to DLQ
                    boolean processed = false;
                    Exception lastError = null;

                    for (int attempt = 1; attempt <= dlqHandler.getMaxRetries(); attempt++) {
                        try {
                            processMessage(root, record.value(), bitcask, archiver, esIndexer);
                            processed = true;
                            break;
                        } catch (Exception e) {
                            lastError = e;
                            if (attempt < dlqHandler.getMaxRetries()) {
                                System.err.printf("[Central] Retry %d/%d for station=%d s_no=%d: %s%n",
                                        attempt, dlqHandler.getMaxRetries(), stationId, sNo, e.getMessage());
                            }
                        }
                    }

                    if (!processed) {
                        dlqHandler.route(record.key(), record.value(), lastError,
                                dlqHandler.getMaxRetries());
                        continue;
                    }

                    messageCount++;
                    if (messageCount % 100 == 0) {
                        System.out.printf(
                                "[Central] Processed %d messages | BitCask keys: %d | Parquet buffer: %d | Dupes skipped: %d%n",
                                messageCount, bitcask.size(), archiver.getBufferedCount(),
                                idempotentReceiver.getDuplicateCount());
                    }
                }
            }
        }
    }

    /**
     * Processes a single weather status message (Envelope Wrapper pattern):
     * The message envelope contains routing metadata (station_id, s_no, timestamp)
     * wrapping the weather payload.
     *
     * 1. Update BitCask with latest status for this station
     * 2. Add to Parquet archive buffer
     * 3. Buffer for ElasticSearch bulk indexing
     */
    private static void processMessage(JsonNode root, String jsonValue,
            BitCaskStore bitcask, ParquetArchiver archiver,
            ElasticIndexer esIndexer) throws Exception {
        long stationId = root.path("station_id").asLong();
        long sNo = root.path("s_no").asLong();
        String batteryStatus = root.path("battery_status").asText();
        long statusTimestamp = root.path("status_timestamp").asLong();
        JsonNode weather = root.path("weather");
        int humidity = weather.path("humidity").asInt();
        int temperature = weather.path("temperature").asInt();
        int windSpeed = weather.path("wind_speed").asInt();

        // 1. Store latest in BitCask (key = station_id as string)
        bitcask.put(String.valueOf(stationId), jsonValue);

        // 2. Archive to Parquet buffer
        WeatherRecord record = new WeatherRecord(
                stationId, sNo, batteryStatus, statusTimestamp,
                humidity, temperature, windSpeed);
        archiver.addRecord(record);

        // 3. Buffer for ElasticSearch (auto-flushes every 30s)
        esIndexer.add(jsonValue);
    }

    private static String env(String key, String defaultValue) {
        return System.getenv().getOrDefault(key, defaultValue);
    }
}
