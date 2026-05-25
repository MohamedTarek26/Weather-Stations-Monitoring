package com.weathermon.station;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Open-Meteo Channel Adapter — Enterprise Integration Patterns
 *
 * Implements two EIP patterns:
 *   1. Channel Adapter — bridges the external Open-Meteo HTTP API to our Kafka messaging system
 *   2. Polling Consumer — uses a ScheduledExecutorService to poll the API at fixed intervals
 *
 * Polls the Open-Meteo free weather API every 30 seconds for real weather data,
 * transforms the response into our standard weather message schema, and publishes
 * to Kafka with station_id=99 (reserved for real data).
 *
 * Environment variables:
 *   STATION_ID       - Station ID for this adapter (default: 99)
 *   KAFKA_BOOTSTRAP  - Kafka broker (default: localhost:9092)
 *   TOPIC            - Kafka topic (default: weather-status)
 *   LATITUDE         - Location latitude (default: 30.06 = Cairo)
 *   LONGITUDE        - Location longitude (default: 31.25 = Cairo)
 *   POLL_INTERVAL    - Polling interval in seconds (default: 30)
 */
public class OpenMeteoAdapter {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private static final AtomicLong sequenceNumber = new AtomicLong(0);

    public static void main(String[] args) {
        int stationId = Integer.parseInt(env("STATION_ID", "99"));
        String bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
        String topic = env("TOPIC", "weather-status");
        double latitude = Double.parseDouble(env("LATITUDE", "30.06"));
        double longitude = Double.parseDouble(env("LONGITUDE", "31.25"));
        int pollInterval = Integer.parseInt(env("POLL_INTERVAL", "30"));

        String apiUrl = String.format(
                "https://api.open-meteo.com/v1/forecast?latitude=%.2f&longitude=%.2f"
                + "&current=temperature_2m,relative_humidity_2m,wind_speed_10m",
                latitude, longitude);

        System.out.println("=== Open-Meteo Channel Adapter (EIP) ===");
        System.out.printf("Station ID: %d%n", stationId);
        System.out.printf("Location: %.2f, %.2f%n", latitude, longitude);
        System.out.printf("API: %s%n", apiUrl);
        System.out.printf("Poll interval: %ds%n", pollInterval);
        System.out.println("=========================================");

        KafkaWeatherProducer producer = new KafkaWeatherProducer(bootstrap, topic);

        // EIP Pattern 2: Polling Consumer — fixed-rate scheduled polling
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "open-meteo-poller");
            t.setDaemon(false);
            return t;
        });

        scheduler.scheduleAtFixedRate(() -> {
            try {
                String weatherJson = pollAndTransform(apiUrl, stationId);
                if (weatherJson != null) {
                    producer.send(stationId, weatherJson);
                    System.out.printf("[OpenMeteo] Published s_no=%d%n", sequenceNumber.get());
                }
            } catch (Exception e) {
                System.err.printf("[OpenMeteo] Poll failed: %s%n", e.getMessage());
            }
        }, 0, pollInterval, TimeUnit.SECONDS);

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[OpenMeteo] Shutting down...");
            scheduler.shutdown();
            producer.close();
        }));
    }

    /**
     * EIP Pattern 1: Channel Adapter
     * Polls the Open-Meteo API and transforms the response to our message schema.
     * Also demonstrates the Envelope Wrapper pattern — wrapping raw weather data
     * inside a metadata envelope (station_id, s_no, timestamp).
     */
    private static String pollAndTransform(String apiUrl, int stationId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(apiUrl))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                System.err.printf("[OpenMeteo] API HTTP %d%n", response.statusCode());
                return null;
            }

            // Parse Open-Meteo response
            JsonNode root = mapper.readTree(response.body());
            JsonNode current = root.path("current");

            int humidity = current.path("relative_humidity_2m").asInt();
            int temperature = (int) Math.round(current.path("temperature_2m").asDouble());
            int windSpeed = (int) Math.round(current.path("wind_speed_10m").asDouble());

            // EIP: Envelope Wrapper — wrap payload with routing metadata
            long sno = sequenceNumber.incrementAndGet();
            ObjectNode message = mapper.createObjectNode();
            message.put("station_id", stationId);
            message.put("s_no", sno);
            message.put("battery_status", "N/A");
            message.put("status_timestamp", System.currentTimeMillis() / 1000);

            ObjectNode weather = mapper.createObjectNode();
            weather.put("humidity", humidity);
            weather.put("temperature", temperature);
            weather.put("wind_speed", windSpeed);
            message.set("weather", weather);

            return mapper.writeValueAsString(message);

        } catch (Exception e) {
            System.err.printf("[OpenMeteo] Error: %s%n", e.getMessage());
            return null;
        }
    }

    private static String env(String key, String defaultValue) {
        return System.getenv().getOrDefault(key, defaultValue);
    }
}
