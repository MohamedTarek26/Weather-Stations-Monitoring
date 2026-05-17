package com.weathermon.station;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Random;

/**
 * Generates randomized weather status messages conforming to the project schema.
 * 
 * Battery distribution: 30% low, 40% medium, 30% high
 * Message drop rate: 10%
 */
public class WeatherDataGenerator {

    private final long stationId;
    private long sequenceNumber;
    private final Random random;
    private final ObjectMapper mapper;

    // Battery distribution thresholds
    private static final double LOW_THRESHOLD = 0.30;      // 0.00 - 0.30 → low (30%)
    private static final double MEDIUM_THRESHOLD = 0.70;    // 0.30 - 0.70 → medium (40%)
    // 0.70 - 1.00 → high (30%)

    private static final double DROP_RATE = 0.10;           // 10% message drop

    // Weather ranges
    private static final int MIN_HUMIDITY = 0;
    private static final int MAX_HUMIDITY = 100;
    private static final int MIN_TEMPERATURE = 30;   // Fahrenheit
    private static final int MAX_TEMPERATURE = 120;
    private static final int MIN_WIND_SPEED = 0;
    private static final int MAX_WIND_SPEED = 150;   // km/h

    public WeatherDataGenerator(long stationId) {
        this.stationId = stationId;
        this.sequenceNumber = 0;
        this.random = new Random();
        this.mapper = new ObjectMapper();
    }

    /**
     * Generates the next weather status message.
     * Returns null if the message should be "dropped" (10% chance).
     * The sequence number increments even on dropped messages.
     */
    public String generateMessage() {
        sequenceNumber++;

        // 10% drop rate
        if (random.nextDouble() < DROP_RATE) {
            System.out.printf("[Station %d] Dropped message s_no=%d%n", stationId, sequenceNumber);
            return null;
        }

        try {
            ObjectNode root = mapper.createObjectNode();
            root.put("station_id", stationId);
            root.put("s_no", sequenceNumber);
            root.put("battery_status", randomBatteryStatus());
            root.put("status_timestamp", System.currentTimeMillis() / 1000);

            ObjectNode weather = mapper.createObjectNode();
            weather.put("humidity", randomInt(MIN_HUMIDITY, MAX_HUMIDITY));
            weather.put("temperature", randomInt(MIN_TEMPERATURE, MAX_TEMPERATURE));
            weather.put("wind_speed", randomInt(MIN_WIND_SPEED, MAX_WIND_SPEED));
            root.set("weather", weather);

            return mapper.writeValueAsString(root);
        } catch (Exception e) {
            System.err.println("Error generating message: " + e.getMessage());
            return null;
        }
    }

    /**
     * Returns battery status with distribution: 30% low, 40% medium, 30% high
     */
    private String randomBatteryStatus() {
        double roll = random.nextDouble();
        if (roll < LOW_THRESHOLD) {
            return "low";
        } else if (roll < MEDIUM_THRESHOLD) {
            return "medium";
        } else {
            return "high";
        }
    }

    private int randomInt(int min, int max) {
        return min + random.nextInt(max - min + 1);
    }

    public long getStationId() {
        return stationId;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }
}
