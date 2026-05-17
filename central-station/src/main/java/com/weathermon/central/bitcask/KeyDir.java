package com.weathermon.central.bitcask;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * KeyDir — In-memory hash index mapping keys to their location on disk.
 *
 * This is what makes BitCask reads O(1): instead of scanning files, you look up
 * the key in this hash map to get (fileId, offset, size), then do one disk seek.
 *
 * Thread-safe via ConcurrentHashMap for concurrent readers from the BitCask client.
 */
public class KeyDir {

    private final ConcurrentHashMap<String, EntryMetadata> map;

    public KeyDir() {
        this.map = new ConcurrentHashMap<>();
    }

    /**
     * Updates the index for a key. Only updates if the new timestamp is >= the existing one.
     * This ensures that during recovery, older entries don't overwrite newer ones.
     */
    public void put(String key, String fileId, long offset, int valueSize, long timestamp) {
        map.merge(key, new EntryMetadata(fileId, offset, valueSize, timestamp),
                (existing, incoming) -> incoming.timestamp() >= existing.timestamp() ? incoming : existing);
    }

    /**
     * Looks up where a key's value is stored on disk.
     * @return EntryMetadata with file location, or null if key not found
     */
    public EntryMetadata get(String key) {
        return map.get(key);
    }

    /** Returns all keys currently stored. */
    public Set<String> keys() {
        return map.keySet();
    }

    /** Returns all entries (for compaction and view-all). */
    public Set<Map.Entry<String, EntryMetadata>> entries() {
        return map.entrySet();
    }

    /** Number of keys stored. */
    public int size() {
        return map.size();
    }

    /** Metadata about where a key's value lives on disk. */
    public record EntryMetadata(String fileId, long offset, int valueSize, long timestamp) {}
}
