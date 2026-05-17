package com.weathermon.central.bitcask;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * CompactionManager — Background segment merging for BitCask.
 *
 * Over time, old segments accumulate stale entries (a key was updated many times,
 * so only the latest value matters). Compaction:
 *   1. Reads all old (inactive) segments
 *   2. For each key, keeps only the newest entry
 *   3. Writes a new compacted segment + hint file
 *   4. Deletes the old segments
 *
 * Runs on a scheduled background thread so it doesn't block active readers/writers.
 */
public class CompactionManager {

    private final Path directory;
    private final KeyDir keyDir;
    private final ScheduledExecutorService scheduler;
    private final long compactionIntervalSeconds;

    // Tracks which segments are old/inactive and eligible for compaction
    private final List<String> inactiveSegmentIds = new CopyOnWriteArrayList<>();

    // Counter for generating compacted segment file IDs
    private int compactedSegmentCounter = 0;

    public CompactionManager(Path directory, KeyDir keyDir, long compactionIntervalSeconds) {
        this.directory = directory;
        this.keyDir = keyDir;
        this.compactionIntervalSeconds = compactionIntervalSeconds;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bitcask-compaction");
            t.setDaemon(true);
            return t;
        });
    }

    /** Marks a segment as inactive (closed for writing, eligible for compaction). */
    public void markInactive(String segmentFileId) {
        inactiveSegmentIds.add(segmentFileId);
    }

    /** Starts the background compaction scheduler. */
    public void start() {
        scheduler.scheduleWithFixedDelay(
                this::runCompaction,
                compactionIntervalSeconds,
                compactionIntervalSeconds,
                TimeUnit.SECONDS
        );
        System.out.println("[Compaction] Scheduled every " + compactionIntervalSeconds + "s");
    }

    /**
     * Runs one round of compaction. Merges all inactive segments into a single
     * new segment, keeping only the latest value for each key.
     */
    private void runCompaction() {
        if (inactiveSegmentIds.size() < 2) {
            return; // not enough segments to compact
        }

        System.out.println("[Compaction] Starting — merging " + inactiveSegmentIds.size() + " segments");

        try {
            // Snapshot the list of segments to compact
            List<String> toCompact = new ArrayList<>(inactiveSegmentIds);

            // Read all entries from old segments, keeping only latest per key
            Map<String, Segment.Entry> latestEntries = new LinkedHashMap<>();

            for (String segId : toCompact) {
                Segment oldSeg = new Segment(directory, segId);
                oldSeg.forEachEntry(entry -> {
                    Segment.Entry existing = latestEntries.get(entry.key());
                    if (existing == null || entry.timestamp() >= existing.timestamp()) {
                        latestEntries.put(entry.key(), entry);
                    }
                });
                oldSeg.close();
            }

            // Write compacted segment + hint file
            compactedSegmentCounter++;
            String compactedId = String.format("compacted_%04d", compactedSegmentCounter);
            Segment compactedSeg = new Segment(directory, compactedId);
            List<HintFile.HintEntry> hintEntries = new ArrayList<>();

            for (Segment.Entry entry : latestEntries.values()) {
                long newOffset = compactedSeg.append(entry.key(), entry.value());
                byte[] valueBytes = entry.value().getBytes("UTF-8");

                // Update KeyDir to point to the new compacted segment
                keyDir.put(entry.key(), compactedId, newOffset, valueBytes.length, entry.timestamp());

                hintEntries.add(new HintFile.HintEntry(
                        entry.timestamp(), entry.key(), valueBytes.length, newOffset));
            }

            // Write hint file for the compacted segment
            HintFile hintFile = new HintFile(directory, compactedId);
            hintFile.write(hintEntries);

            compactedSeg.close();

            // Delete old segment files and their hint files
            for (String segId : toCompact) {
                Files.deleteIfExists(directory.resolve(segId + ".data"));
                Files.deleteIfExists(directory.resolve(segId + ".hint"));
                inactiveSegmentIds.remove(segId);
            }

            // The compacted segment is itself now inactive (unless it's the only one)
            inactiveSegmentIds.add(compactedId);

            System.out.printf("[Compaction] Done — merged %d segments into %s (%d keys)%n",
                    toCompact.size(), compactedId, latestEntries.size());

        } catch (Exception e) {
            System.err.println("[Compaction] Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void shutdown() {
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
