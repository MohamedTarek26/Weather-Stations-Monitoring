package com.weathermon.station;

/**
 * Weather Station Application - Entry Point
 * 
 * Simulates an IoT weather station that:
 * 1. Generates a weather status message every 1 second
 * 2. Randomly drops 10% of messages (simulating network issues)
 * 3. Sends non-dropped messages to Kafka topic "weather-status"
 * 
 * Configuration via environment variables:
 * STATION_ID - Unique station identifier (default: 1)
 * KAFKA_BOOTSTRAP - Kafka bootstrap servers (default: localhost:9092)
 * TOPIC - Kafka topic name (default: weather-status)
 */
public class WeatherStationApp {

    public static void main(String[] args) {
        // Read configuration from environment or defaults
        long stationId = Long.parseLong(System.getenv().getOrDefault("STATION_ID", "1"));
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        String topic = System.getenv().getOrDefault("TOPIC", "weather-status");

        System.out.printf("=== Weather Station %d starting ===%n", stationId);
        System.out.printf("Kafka: %s | Topic: %s%n", bootstrap, topic);
        System.out.println("Sending 1 message/second (10%% drop rate)");
        System.out.println("Battery distribution: 30%% low / 40%% medium / 30%% high");
        System.out.println("=========================================");

        WeatherDataGenerator generator = new WeatherDataGenerator(stationId);

        try (KafkaWeatherProducer producer = new KafkaWeatherProducer(bootstrap, topic)) {
            // Register shutdown hook for graceful termination
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.printf("%n[Station %d] Shutting down...%n", stationId);
            }));

            // Main loop: generate and send messages every 1 second
            while (true) {
                String message = generator.generateMessage();

                if (message != null) {
                    producer.send(stationId, message);
                }
                // message == null means it was "dropped"

                Thread.sleep(1000); // 1 second interval
            }
        } catch (InterruptedException e) {
            System.out.printf("[Station %d] Interrupted, stopping.%n", stationId);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.printf("[Station %d] Fatal error: %s%n", stationId, e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
