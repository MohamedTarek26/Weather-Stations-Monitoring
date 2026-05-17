package com.weathermon.central.eip;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * EIP Pattern: Idempotent Receiver
 *
 * Deduplicates incoming messages using a composite key of (station_id, s_no).
 * Uses a bounded LRU cache to track recently processed message IDs, preventing
 * duplicate processing without unbounded memory growth.
 *
 * This is important when Kafka rebalances or restarts cause messages to be
 * re-delivered (at-least-once semantics).
 */
public class IdempotentReceiver {

    private final LinkedHashMap<String, Boolean> recentKeys;
    private long duplicateCount = 0;

    /**
     * @param capacity maximum number of message IDs to remember
     */
    public IdempotentReceiver(int capacity) {
        // LRU eviction: when map exceeds capacity, oldest entry is removed
        this.recentKeys = new LinkedHashMap<>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > capacity;
            }
        };
        System.out.printf("[EIP] Idempotent Receiver initialized (capacity: %d)%n", capacity);
    }

    /**
     * Checks if a message with the given (stationId, sNo) has already been
     * processed.
     *
     * @param stationId the station ID
     * @param sNo       the sequence number
     * @return true if this is a DUPLICATE (should be skipped), false if new
     */
    public synchronized boolean isDuplicate(long stationId, long sNo) {
        String dedupeKey = stationId + ":" + sNo;

        if (recentKeys.containsKey(dedupeKey)) {
            duplicateCount++;
            System.out.printf("[EIP-Idempotent] Duplicate skipped: station=%d s_no=%d (total dupes: %d)%n",
                    stationId, sNo, duplicateCount);
            return true;
        }

        recentKeys.put(dedupeKey, Boolean.TRUE);
        return false;
    }

    public long getDuplicateCount() {
        return duplicateCount;
    }
}
