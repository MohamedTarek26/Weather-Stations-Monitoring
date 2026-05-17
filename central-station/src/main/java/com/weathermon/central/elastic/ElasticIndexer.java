package com.weathermon.central.elastic;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ElasticSearch Bulk Indexer
 *
 * Buffers weather messages and bulk-indexes them to ES every 30 seconds.
 * Uses Java's built-in HttpClient — no external ES library needed.
 */
public class ElasticIndexer {

    private final String esUrl;
    private final String indexName;
    private final HttpClient httpClient;
    private final List<String> buffer = new ArrayList<>();
    private final ScheduledExecutorService scheduler;
    private volatile boolean running = true;

    private static final int FLUSH_INTERVAL_SECONDS = 30;

    public ElasticIndexer(String esHost, String indexName) {
        this.esUrl = "http://" + esHost;
        this.indexName = indexName;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        // Create index with mappings on startup
        createIndex();

        // Schedule periodic flush
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "es-indexer");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(
                this::flush,
                FLUSH_INTERVAL_SECONDS,
                FLUSH_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );

        System.out.printf("[ES] Indexer started → %s/%s (flush every %ds)%n",
                esUrl, indexName, FLUSH_INTERVAL_SECONDS);
    }

    /**
     * Add a raw JSON weather message to the buffer.
     */
    public synchronized void add(String jsonMessage) {
        if (running) {
            buffer.add(jsonMessage);
        }
    }

    /**
     * Flush buffered messages to ES via the Bulk API.
     */
    public synchronized void flush() {
        if (buffer.isEmpty()) return;

        List<String> toFlush = new ArrayList<>(buffer);
        buffer.clear();

        try {
            StringBuilder bulk = new StringBuilder();
            for (String doc : toFlush) {
                bulk.append("{\"index\":{\"_index\":\"").append(indexName).append("\"}}\n");
                bulk.append(doc).append("\n");
            }

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(esUrl + "/_bulk"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(bulk.toString()))
                    .timeout(Duration.ofSeconds(10))
                    .build();

            HttpResponse<String> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                System.out.printf("[ES] Indexed %d documents%n", toFlush.size());
            } else {
                System.err.printf("[ES] Bulk index failed (HTTP %d): %s%n",
                        response.statusCode(), response.body().substring(0, Math.min(200, response.body().length())));
            }
        } catch (Exception e) {
            System.err.printf("[ES] Bulk index error: %s%n", e.getMessage());
            // Re-add failed messages to buffer for retry
            synchronized (this) {
                buffer.addAll(0, toFlush);
            }
        }
    }

    /**
     * Create the ES index with proper field mappings (idempotent).
     */
    private void createIndex() {
        String mapping = """
                {
                  "mappings": {
                    "properties": {
                      "station_id":       {"type": "long"},
                      "s_no":             {"type": "long"},
                      "battery_status":   {"type": "keyword"},
                      "status_timestamp": {"type": "long"},
                      "weather": {
                        "properties": {
                          "humidity":     {"type": "integer"},
                          "temperature":  {"type": "integer"},
                          "wind_speed":   {"type": "integer"}
                        }
                      }
                    }
                  }
                }
                """;

        try {
            // Check if index exists
            HttpRequest check = HttpRequest.newBuilder()
                    .uri(URI.create(esUrl + "/" + indexName))
                    .method("HEAD", HttpRequest.BodyPublishers.noBody())
                    .timeout(Duration.ofSeconds(5))
                    .build();
            HttpResponse<Void> resp = httpClient.send(check, HttpResponse.BodyHandlers.discarding());

            if (resp.statusCode() == 200) {
                System.out.printf("[ES] Index '%s' already exists%n", indexName);
                return;
            }

            // Create index
            HttpRequest create = HttpRequest.newBuilder()
                    .uri(URI.create(esUrl + "/" + indexName))
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString(mapping))
                    .timeout(Duration.ofSeconds(10))
                    .build();
            HttpResponse<String> createResp = httpClient.send(create, HttpResponse.BodyHandlers.ofString());

            if (createResp.statusCode() == 200) {
                System.out.printf("[ES] Created index '%s'%n", indexName);
            } else {
                System.err.printf("[ES] Failed to create index: %s%n", createResp.body());
            }
        } catch (Exception e) {
            System.err.printf("[ES] Could not connect to ES at %s: %s%n", esUrl, e.getMessage());
            System.err.println("[ES] Indexer will retry on next flush cycle");
        }
    }

    /**
     * Shutdown: flush remaining and stop scheduler.
     */
    public void close() {
        running = false;
        flush();
        scheduler.shutdown();
    }
}
