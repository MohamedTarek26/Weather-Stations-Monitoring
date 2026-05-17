package com.weathermon.central.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weathermon.central.bitcask.BitCaskStore;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

/**
 * BitCaskServer — Lightweight HTTP API for the BitCask CLI client.
 *
 * Uses Java's built-in HttpServer (no external dependencies needed).
 *
 * Endpoints:
 *   GET /view-all         → returns all keys and their latest values as JSON
 *   GET /view?key=X       → returns the value for a single key
 *   GET /keys             → returns a list of all keys
 *   GET /health           → health check
 */
public class BitCaskServer {

    private final BitCaskStore bitcask;
    private final int port;
    private final ObjectMapper mapper = new ObjectMapper();
    private HttpServer server;

    public BitCaskServer(BitCaskStore bitcask, int port) {
        this.bitcask = bitcask;
        this.port = port;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(10));

        server.createContext("/view-all", this::handleViewAll);
        server.createContext("/view", this::handleView);
        server.createContext("/keys", this::handleKeys);
        server.createContext("/health", this::handleHealth);

        server.start();
        System.out.printf("[API] HTTP server started on port %d%n", port);
    }

    /**
     * GET /view-all
     * Returns all keys with their latest values as JSON: {"key1": "value1", "key2": "value2"}
     */
    private void handleViewAll(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("GET")) {
            sendResponse(exchange, 405, "Method Not Allowed");
            return;
        }
        try {
            Map<String, String> all = bitcask.getAll();
            String json = mapper.writeValueAsString(all);
            sendResponse(exchange, 200, json);
        } catch (Exception e) {
            sendResponse(exchange, 500, "Error: " + e.getMessage());
        }
    }

    /**
     * GET /view?key=X
     * Returns the value for a single key.
     */
    private void handleView(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("GET")) {
            sendResponse(exchange, 405, "Method Not Allowed");
            return;
        }

        // Parse query parameter
        String query = exchange.getRequestURI().getQuery();
        String key = null;
        if (query != null) {
            for (String param : query.split("&")) {
                String[] parts = param.split("=", 2);
                if (parts.length == 2 && parts[0].equals("key")) {
                    key = parts[1];
                }
            }
        }

        if (key == null) {
            sendResponse(exchange, 400, "Missing 'key' parameter. Usage: /view?key=SOME_KEY");
            return;
        }

        try {
            String value = bitcask.get(key);
            if (value == null) {
                sendResponse(exchange, 404, "Key not found: " + key);
            } else {
                sendResponse(exchange, 200, value);
            }
        } catch (Exception e) {
            sendResponse(exchange, 500, "Error: " + e.getMessage());
        }
    }

    /**
     * GET /keys
     * Returns all keys as a JSON array: ["key1", "key2", ...]
     */
    private void handleKeys(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("GET")) {
            sendResponse(exchange, 405, "Method Not Allowed");
            return;
        }
        try {
            Set<String> keys = bitcask.listKeys();
            String json = mapper.writeValueAsString(keys);
            sendResponse(exchange, 200, json);
        } catch (Exception e) {
            sendResponse(exchange, 500, "Error: " + e.getMessage());
        }
    }

    /**
     * GET /health
     * Simple health check.
     */
    private void handleHealth(HttpExchange exchange) throws IOException {
        sendResponse(exchange, 200, "{\"status\":\"ok\",\"keys\":" + bitcask.size() + "}");
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] bytes = body.getBytes("UTF-8");
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    public void stop() {
        if (server != null) {
            server.stop(2);
            System.out.println("[API] HTTP server stopped.");
        }
    }
}
