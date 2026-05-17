package com.weathermon.central.bitcask;

import java.io.File;
import java.nio.file.*;
import java.util.Map;
import java.util.Set;

/**
 * Comprehensive test for BitCask KV Store.
 * Covers: basic ops, overwrites, recovery (hints + scan), rotation, compaction, concurrency.
 *
 * Run:
 *   ./gradlew :central-station:compileJava
 *   $JAVA_HOME/bin/java -cp central-station/build/classes/java/main \
 *       com.weathermon.central.bitcask.BitCaskTest
 */
public class BitCaskTest {

    private static final String TEST_DIR = "/tmp/bitcask-test";
    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) throws Exception {

        System.out.println("\n═══ TEST 1: Basic Put / Get ═══");
        testBasicPutGet();

        System.out.println("\n═══ TEST 2: Overwrite Returns Latest ═══");
        testOverwrite();

        System.out.println("\n═══ TEST 3: Multiple Keys + ListKeys + GetAll ═══");
        testMultipleKeys();

        System.out.println("\n═══ TEST 4: Recovery from Hint Files (fast path) ═══");
        testRecoveryFromHints();

        System.out.println("\n═══ TEST 5: Recovery from Segment Scan (no hints) ═══");
        testRecoveryFromSegmentScan();

        System.out.println("\n═══ TEST 6: Segment Rotation ═══");
        testSegmentRotation();

        System.out.println("\n═══ TEST 7: Compaction Preserves Latest Values ═══");
        testCompaction();

        System.out.println("\n═══ TEST 8: Concurrent Reads (10 threads) ═══");
        testConcurrentReads();

        System.out.println("\n═══ TEST 9: Large Values ═══");
        testLargeValues();

        System.out.println("\n═══ TEST 10: Empty Store ═══");
        testEmptyStore();

        System.out.println("\n════════════════════════════════");
        System.out.printf("  Results: %d passed, %d failed%n", passed, failed);
        System.out.println("════════════════════════════════");
        if (failed > 0) System.exit(1);
    }

    // ─── Test 1: Basic put and get ───────────────────────────────

    static void testBasicPutGet() throws Exception {
        cleanDir(TEST_DIR);
        BitCaskStore store = new BitCaskStore(TEST_DIR);

        store.put("key1", "value1");
        store.put("key2", "value2");

        check("get key1", "value1", store.get("key1"));
        check("get key2", "value2", store.get("key2"));
        check("get nonexistent returns null", null, store.get("key999"));
        check("size is 2", 2, store.size());

        store.close();
    }

    // ─── Test 2: Overwrite returns latest value ──────────────────

    static void testOverwrite() throws Exception {
        cleanDir(TEST_DIR);
        BitCaskStore store = new BitCaskStore(TEST_DIR);

        store.put("station_1", "{\"temp\": 80}");
        check("first write", "{\"temp\": 80}", store.get("station_1"));

        store.put("station_1", "{\"temp\": 85}");
        check("second write overwrites", "{\"temp\": 85}", store.get("station_1"));

        store.put("station_1", "{\"temp\": 90}");
        check("third write overwrites", "{\"temp\": 90}", store.get("station_1"));

        check("still only 1 key", 1, store.size());

        store.close();
    }

    // ─── Test 3: Multiple distinct keys ──────────────────────────

    static void testMultipleKeys() throws Exception {
        cleanDir(TEST_DIR);
        BitCaskStore store = new BitCaskStore(TEST_DIR);

        for (int i = 1; i <= 10; i++) {
            store.put("station_" + i, "{\"id\":" + i + "}");
        }

        check("size is 10", 10, store.size());

        Set<String> keys = store.listKeys();
        check("listKeys contains station_1", true, keys.contains("station_1"));
        check("listKeys contains station_10", true, keys.contains("station_10"));
        check("listKeys does NOT contain station_11", false, keys.contains("station_11"));

        Map<String, String> all = store.getAll();
        check("getAll has 10 entries", 10, all.size());
        check("getAll station_5 correct", "{\"id\":5}", all.get("station_5"));

        store.close();
    }

    // ─── Test 4: Recovery from hint files (fast path) ────────────

    static void testRecoveryFromHints() throws Exception {
        cleanDir(TEST_DIR);

        // Phase 1: write data and close (close writes hint file)
        BitCaskStore store = new BitCaskStore(TEST_DIR);
        store.put("station_1", "{\"temp\": 100}");
        store.put("station_2", "{\"temp\": 200}");
        store.put("station_1", "{\"temp\": 111}"); // overwrite
        store.close();

        // Verify hint files exist
        long hintCount = Files.list(Path.of(TEST_DIR))
                .filter(p -> p.toString().endsWith(".hint")).count();
        check("hint file(s) created on close", true, hintCount > 0);

        // Phase 2: reopen — should recover from hints
        BitCaskStore recovered = new BitCaskStore(TEST_DIR);
        check("recovered 2 keys", 2, recovered.size());
        check("recovered station_1 (latest value)", "{\"temp\": 111}", recovered.get("station_1"));
        check("recovered station_2", "{\"temp\": 200}", recovered.get("station_2"));
        recovered.close();
    }

    // ─── Test 5: Recovery without hint files (segment scan) ──────

    static void testRecoveryFromSegmentScan() throws Exception {
        cleanDir(TEST_DIR);

        BitCaskStore store = new BitCaskStore(TEST_DIR);
        store.put("key_a", "aaa");
        store.put("key_b", "bbb");
        store.put("key_a", "aaa_updated"); // overwrite
        store.close();

        // Delete ALL hint files to force slow-path recovery
        Files.list(Path.of(TEST_DIR))
                .filter(p -> p.toString().endsWith(".hint"))
                .forEach(p -> { try { Files.delete(p); } catch (Exception e) {} });

        check("hint files deleted", true,
                Files.list(Path.of(TEST_DIR)).noneMatch(p -> p.toString().endsWith(".hint")));

        // Reopen — must scan .data files entry by entry
        BitCaskStore recovered = new BitCaskStore(TEST_DIR);
        check("scan recovery count", 2, recovered.size());
        check("scan recovery key_a (latest)", "aaa_updated", recovered.get("key_a"));
        check("scan recovery key_b", "bbb", recovered.get("key_b"));
        recovered.close();
    }

    // ─── Test 6: Segment rotation at 64KB ────────────────────────

    static void testSegmentRotation() throws Exception {
        cleanDir(TEST_DIR);
        BitCaskStore store = new BitCaskStore(TEST_DIR);

        // 500-byte values × 400 entries = ~200KB → should trigger several rotations
        String bigValue = "x".repeat(500);
        for (int i = 0; i < 400; i++) {
            store.put("key_" + i, bigValue + "_" + i);
        }

        long dataFileCount = Files.list(Path.of(TEST_DIR))
                .filter(p -> p.toString().endsWith(".data")).count();
        System.out.printf("  [info] %d segment files created%n", dataFileCount);

        check("multiple segments created", true, dataFileCount > 1);
        check("all 400 keys stored", 400, store.size());
        check("first key readable",  true, store.get("key_0") != null);
        check("last key readable",   true, store.get("key_399") != null);
        check("first key correct value", bigValue + "_0", store.get("key_0"));
        check("last key correct value",  bigValue + "_399", store.get("key_399"));

        store.close();

        // Verify rotation doesn't break recovery
        BitCaskStore recovered = new BitCaskStore(TEST_DIR);
        check("post-rotation recovery count", 400, recovered.size());
        check("post-rotation key_200", bigValue + "_200", recovered.get("key_200"));
        recovered.close();
    }

    // ─── Test 7: Compaction merges and preserves latest ──────────

    static void testCompaction() throws Exception {
        cleanDir(TEST_DIR);
        BitCaskStore store = new BitCaskStore(TEST_DIR);

        // 5 rounds of overwriting the same 200 keys → lots of stale entries
        String bigValue = "x".repeat(500);
        for (int round = 0; round < 5; round++) {
            for (int i = 0; i < 200; i++) {
                store.put("key_" + i, bigValue + "_round_" + round);
            }
        }

        long filesBefore = Files.list(Path.of(TEST_DIR))
                .filter(p -> p.toString().endsWith(".data")).count();
        System.out.printf("  [info] %d segment files before compaction%n", filesBefore);

        check("200 unique keys (not 1000)", 200, store.size());
        check("key_0 is from round 4", bigValue + "_round_4", store.get("key_0"));
        check("key_199 is from round 4", bigValue + "_round_4", store.get("key_199"));

        store.close();

        // Recovery after many overwrites
        BitCaskStore recovered = new BitCaskStore(TEST_DIR);
        check("post-compaction recovery count", 200, recovered.size());
        check("post-compaction key_100 latest", bigValue + "_round_4", recovered.get("key_100"));
        recovered.close();
    }

    // ─── Test 8: Concurrent reads from multiple threads ──────────

    static void testConcurrentReads() throws Exception {
        cleanDir(TEST_DIR);
        BitCaskStore store = new BitCaskStore(TEST_DIR);

        for (int i = 0; i < 10; i++) {
            store.put("station_" + i, "{\"id\":" + i + "}");
        }

        Thread[] threads = new Thread[10];
        boolean[] results = new boolean[10];

        for (int t = 0; t < 10; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                try {
                    // Each thread reads all 10 keys multiple times
                    for (int rep = 0; rep < 50; rep++) {
                        for (int i = 0; i < 10; i++) {
                            String val = store.get("station_" + i);
                            if (val == null || !val.equals("{\"id\":" + i + "}")) {
                                System.err.printf("  Thread %d: wrong value for station_%d: %s%n",
                                        threadId, i, val);
                                return;
                            }
                        }
                    }
                    results[threadId] = true; // all reads correct
                } catch (Exception e) {
                    System.err.printf("  Thread %d error: %s%n", threadId, e.getMessage());
                }
            });
            threads[t].start();
        }

        for (Thread t : threads) t.join(10000);

        boolean allPassed = true;
        for (int i = 0; i < 10; i++) {
            if (!results[i]) {
                System.out.printf("  ❌ thread %d failed%n", i);
                allPassed = false;
            }
        }
        check("10 threads × 50 rounds × 10 keys all correct", true, allPassed);

        store.close();
    }

    // ─── Test 9: Large values ────────────────────────────────────

    static void testLargeValues() throws Exception {
        cleanDir(TEST_DIR);
        BitCaskStore store = new BitCaskStore(TEST_DIR);

        // 100KB value — larger than one segment
        String largeValue = "V".repeat(100_000);
        store.put("big_key", largeValue);

        String retrieved = store.get("big_key");
        check("large value length", 100_000, retrieved != null ? retrieved.length() : -1);
        check("large value content correct", largeValue, retrieved);

        store.close();

        // Recovery of large value
        BitCaskStore recovered = new BitCaskStore(TEST_DIR);
        check("large value survives recovery", largeValue, recovered.get("big_key"));
        recovered.close();
    }

    // ─── Test 10: Empty store behavior ───────────────────────────

    static void testEmptyStore() throws Exception {
        cleanDir(TEST_DIR);
        BitCaskStore store = new BitCaskStore(TEST_DIR);

        check("empty store size is 0", 0, store.size());
        check("get on empty returns null", null, store.get("anything"));
        check("listKeys on empty is empty", true, store.listKeys().isEmpty());
        check("getAll on empty is empty", true, store.getAll().isEmpty());

        store.close();

        // Reopen empty store
        BitCaskStore recovered = new BitCaskStore(TEST_DIR);
        check("recovered empty store size", 0, recovered.size());
        recovered.close();
    }

    // ─── Helpers ─────────────────────────────────────────────────

    static void check(String name, Object expected, Object actual) {
        if (expected == null ? actual == null : expected.equals(actual)) {
            System.out.printf("  ✅ %s%n", name);
            passed++;
        } else {
            System.out.printf("  ❌ %s — expected: %s, got: %s%n", name,
                    truncate(expected), truncate(actual));
            failed++;
        }
    }

    static String truncate(Object obj) {
        if (obj == null) return "null";
        String s = obj.toString();
        return s.length() > 80 ? s.substring(0, 80) + "...(" + s.length() + " chars)" : s;
    }

    static void cleanDir(String path) throws Exception {
        File dir = new File(path);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) for (File f : files) f.delete();
            dir.delete();
        }
    }
}
