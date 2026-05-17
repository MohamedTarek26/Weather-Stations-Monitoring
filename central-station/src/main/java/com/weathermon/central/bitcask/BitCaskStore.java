package com.weathermon.central.bitcask;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * BitCaskStore — The main facade for the BitCask key-value store.
 *
 * Provides put(key, value), get(key), and listKeys() operations.
 * Manages:
 *   - Active segment rotation (when segment exceeds size threshold)
 *   - Startup recovery from hint files
 *   - Background compaction scheduling
 *
 * Usage:
 *   BitCaskStore store = new BitCaskStore("/path/to/data");
 *   store.put("station_1", "{...json...}");
 *   String value = store.get("station_1");
 *   store.close();
 */
public class BitCaskStore {

    private static final long MAX_SEGMENT_SIZE = 64 * 1024; // 64KB per segment before rotation
    private static final long COMPACTION_INTERVAL_SECONDS = 30;

    private final Path directory;
    private final KeyDir keyDir;
    private final CompactionManager compactionManager;

    private Segment activeSegment;
    private int segmentCounter;

    public BitCaskStore(String directoryPath) throws IOException {
        this.directory = Path.of(directoryPath);
        Files.createDirectories(directory);
        this.keyDir = new KeyDir();
        this.compactionManager = new CompactionManager(directory, keyDir, COMPACTION_INTERVAL_SECONDS);
        this.segmentCounter = 0;

        // Recover existing data from hint files / segments
        recover();

        // Open a new active segment for writes
        this.activeSegment = createNewSegment();

        // Start background compaction
        compactionManager.start();

        System.out.printf("[BitCask] Ready — %d keys recovered, active segment: %s%n",
                keyDir.size(), activeSegment.getFileId());
    }

    /**
     * Stores a key-value pair. Appends to the active segment and updates the KeyDir.
     * If the segment is full, rotates to a new one.
     */
    public synchronized void put(String key, String value) throws IOException {
        // Check if active segment needs rotation
        if (activeSegment.size() >= MAX_SEGMENT_SIZE) {
            rotateSegment();
        }

        long offset = activeSegment.append(key, value);
        int valueSize = value.getBytes("UTF-8").length;
        keyDir.put(key, activeSegment.getFileId(), offset, valueSize, System.currentTimeMillis());
    }

    /**
     * Retrieves the latest value for a key.
     * @return the value string, or null if key not found
     */
    public String get(String key) throws IOException {
        KeyDir.EntryMetadata meta = keyDir.get(key);
        if (meta == null) {
            return null;
        }

        // Open the segment file and read from the stored offset
        Segment segment = new Segment(directory, meta.fileId());
        try {
            return segment.readValue(meta.offset());
        } finally {
            segment.close();
        }
    }

    /** Returns all keys currently stored. */
    public Set<String> listKeys() {
        return keyDir.keys();
    }

    /** Returns all key-value pairs (for view-all). */
    public Map<String, String> getAll() throws IOException {
        Map<String, String> result = new LinkedHashMap<>();
        for (String key : keyDir.keys()) {
            String value = get(key);
            if (value != null) {
                result.put(key, value);
            }
        }
        return result;
    }

    /** Number of keys stored. */
    public int size() {
        return keyDir.size();
    }

    /**
     * Recovers the KeyDir from existing hint files and segment files on startup.
     * Priority: hint files (fast) > segment scanning (slow fallback).
     */
    private void recover() throws IOException {
        if (!Files.exists(directory)) return;

        // Find all segment data files
        List<String> segmentIds;
        try (Stream<Path> paths = Files.list(directory)) {
            segmentIds = paths
                    .filter(p -> p.toString().endsWith(".data"))
                    .map(p -> p.getFileName().toString().replace(".data", ""))
                    .sorted()
                    .collect(Collectors.toList());
        }

        if (segmentIds.isEmpty()) {
            System.out.println("[BitCask] No existing data found, starting fresh.");
            return;
        }

        System.out.printf("[BitCask] Recovering from %d segment(s)...%n", segmentIds.size());

        for (String segId : segmentIds) {
            HintFile hintFile = new HintFile(directory, segId);

            if (hintFile.exists()) {
                // Fast path: read from hint file
                List<HintFile.HintEntry> hints = hintFile.read();
                for (HintFile.HintEntry hint : hints) {
                    keyDir.put(hint.key(), segId, hint.dataOffset(), hint.valueSize(), hint.timestamp());
                }
                System.out.printf("[BitCask] Recovered %d keys from hint: %s%n", hints.size(), segId);
            } else {
                // Slow path: scan the full segment
                Segment segment = new Segment(directory, segId);
                segment.forEachEntry(entry -> {
                    int valueSize = entry.value().getBytes("UTF-8").length;
                    keyDir.put(entry.key(), segId, entry.offset(), valueSize, entry.timestamp());
                });
                segment.close();
                System.out.printf("[BitCask] Recovered keys from segment scan: %s%n", segId);
            }

            // Track the segment counter to avoid ID collisions
            try {
                String numPart = segId.replaceAll("[^0-9]", "");
                if (!numPart.isEmpty()) {
                    segmentCounter = Math.max(segmentCounter, Integer.parseInt(numPart));
                }
            } catch (NumberFormatException ignored) {}

            // Mark old segments as inactive for compaction
            compactionManager.markInactive(segId);
        }
    }

    /** Closes the active segment and opens a new one. */
    private void rotateSegment() throws IOException {
        // Write hint file for the closing segment
        writeHintForSegment(activeSegment);

        // Mark the old segment as inactive for compaction
        compactionManager.markInactive(activeSegment.getFileId());
        activeSegment.close();

        // Create new active segment
        activeSegment = createNewSegment();
        System.out.printf("[BitCask] Rotated to new segment: %s%n", activeSegment.getFileId());
    }

    /** Creates a new segment file with an incrementing ID. */
    private Segment createNewSegment() throws IOException {
        segmentCounter++;
        String fileId = String.format("segment_%04d", segmentCounter);
        return new Segment(directory, fileId);
    }

    /** Writes a hint file for a segment by scanning its entries. */
    private void writeHintForSegment(Segment segment) throws IOException {
        List<HintFile.HintEntry> hints = new ArrayList<>();
        segment.forEachEntry(entry -> {
            int valueSize = entry.value().getBytes("UTF-8").length;
            hints.add(new HintFile.HintEntry(entry.timestamp(), entry.key(), valueSize, entry.offset()));
        });

        HintFile hintFile = new HintFile(directory, segment.getFileId());
        hintFile.write(hints);
    }

    /** Closes the store, flushing all data. */
    public void close() throws IOException {
        compactionManager.shutdown();
        writeHintForSegment(activeSegment);
        activeSegment.close();
        System.out.println("[BitCask] Closed.");
    }
}
