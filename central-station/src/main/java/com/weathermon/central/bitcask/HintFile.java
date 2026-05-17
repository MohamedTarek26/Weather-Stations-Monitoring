package com.weathermon.central.bitcask;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

/**
 * HintFile — Compact index for fast recovery.
 *
 * Instead of scanning full data segment files on startup, the KeyDir is rebuilt
 * from hint files which contain only the metadata (no values).
 *
 * Hint record format:
 * ┌───────────┬──────────┬────────────┬────────────────┬─────────┐
 * │ timestamp │ key_size │ value_size │ data_offset    │   key   │
 * │  8 bytes  │ 4 bytes  │  4 bytes   │   8 bytes      │ N bytes │
 * └───────────┴──────────┴────────────┴────────────────┴─────────┘
 *
 * The value_size is stored so the KeyDir knows how many bytes to read from the
 * data file, but the actual value is NOT stored in the hint file (that's the point).
 */
public class HintFile {

    private static final int HINT_HEADER_SIZE = 24; // timestamp(8) + key_size(4) + value_size(4) + offset(8)

    private final Path filePath;

    public HintFile(Path directory, String fileId) {
        this.filePath = directory.resolve(fileId + ".hint");
    }

    /**
     * Writes hint entries for a segment. Called after compaction or when closing a segment.
     */
    public void write(List<HintEntry> entries) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(filePath.toFile());
             FileChannel channel = fos.getChannel()) {

            for (HintEntry entry : entries) {
                byte[] keyBytes = entry.key().getBytes("UTF-8");
                int recordSize = HINT_HEADER_SIZE + keyBytes.length;

                ByteBuffer buffer = ByteBuffer.allocate(recordSize);
                buffer.putLong(entry.timestamp());
                buffer.putInt(keyBytes.length);
                buffer.putInt(entry.valueSize());
                buffer.putLong(entry.dataOffset());
                buffer.put(keyBytes);
                buffer.flip();

                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
            }
        }
    }

    /**
     * Reads all hint entries from this file. Used during startup recovery.
     */
    public List<HintEntry> read() throws IOException {
        List<HintEntry> entries = new ArrayList<>();

        if (!Files.exists(filePath)) {
            return entries;
        }

        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r");
             FileChannel channel = raf.getChannel()) {

            long fileSize = channel.size();
            long offset = 0;

            while (offset < fileSize) {
                ByteBuffer headerBuf = ByteBuffer.allocate(HINT_HEADER_SIZE);
                int read = channel.read(headerBuf, offset);
                if (read < HINT_HEADER_SIZE) break;
                headerBuf.flip();

                long timestamp = headerBuf.getLong();
                int keySize = headerBuf.getInt();
                int valueSize = headerBuf.getInt();
                long dataOffset = headerBuf.getLong();

                ByteBuffer keyBuf = ByteBuffer.allocate(keySize);
                channel.read(keyBuf, offset + HINT_HEADER_SIZE);
                keyBuf.flip();
                String key = new String(keyBuf.array(), "UTF-8");

                entries.add(new HintEntry(timestamp, key, valueSize, dataOffset));
                offset += HINT_HEADER_SIZE + keySize;
            }
        }

        return entries;
    }

    public boolean exists() {
        return Files.exists(filePath);
    }

    public Path getFilePath() {
        return filePath;
    }

    /** A single hint entry — metadata only, no value. */
    public record HintEntry(long timestamp, String key, int valueSize, long dataOffset) {}
}
