package com.weathermon.central.bitcask;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

/**
 * Segment — An append-only data file for BitCask.
 *
 * Binary record format (each entry):
 * ┌───────────┬──────────┬────────────┬─────────┬─────────┐
 * │ timestamp │ key_size │ value_size │   key   │  value  │
 * │  8 bytes  │ 4 bytes  │  4 bytes   │ N bytes │ M bytes │
 * └───────────┴──────────┴────────────┴─────────┴─────────┘
 *
 * Header = 16 bytes (8 + 4 + 4), then key bytes, then value bytes.
 */
public class Segment {

    public static final int HEADER_SIZE = 16; // timestamp(8) + key_size(4) + value_size(4)

    private final String fileId;
    private final Path filePath;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private long currentOffset;

    /**
     * Opens or creates a segment file.
     * @param directory the BitCask data directory
     * @param fileId unique identifier for this segment (e.g., "segment_0001")
     */
    public Segment(Path directory, String fileId) throws IOException {
        this.fileId = fileId;
        this.filePath = directory.resolve(fileId + ".data");
        Files.createDirectories(directory);
        this.raf = new RandomAccessFile(filePath.toFile(), "rw");
        this.channel = raf.getChannel();
        this.currentOffset = raf.length(); // go to end if file exists (for recovery)
    }

    /**
     * Appends a key-value entry to this segment.
     * @return the byte offset where this entry was written (used for KeyDir)
     */
    public synchronized long append(String key, String value) throws IOException {
        long writeOffset = currentOffset;
        long timestamp = System.currentTimeMillis();

        byte[] keyBytes = key.getBytes("UTF-8");
        byte[] valueBytes = value.getBytes("UTF-8");

        // Build the record: [timestamp | key_size | value_size | key | value]
        int recordSize = HEADER_SIZE + keyBytes.length + valueBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(recordSize);
        buffer.putLong(timestamp);
        buffer.putInt(keyBytes.length);
        buffer.putInt(valueBytes.length);
        buffer.put(keyBytes);
        buffer.put(valueBytes);
        buffer.flip();

        // Write at current offset
        channel.position(currentOffset);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }

        currentOffset += recordSize;
        return writeOffset;
    }

    /**
     * Reads a value from a specific offset in this segment.
     * @param offset the byte offset of the record
     * @return the value string, or null if read failed
     */
    public String readValue(long offset) throws IOException {
        ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE);
        channel.read(headerBuf, offset);
        headerBuf.flip();

        long timestamp = headerBuf.getLong();
        int keySize = headerBuf.getInt();
        int valueSize = headerBuf.getInt();

        // Skip past the key, read only the value
        ByteBuffer valueBuf = ByteBuffer.allocate(valueSize);
        channel.read(valueBuf, offset + HEADER_SIZE + keySize);
        valueBuf.flip();

        return new String(valueBuf.array(), "UTF-8");
    }

    /**
     * Reads a full entry (key + value + metadata) from a specific offset.
     * Used during recovery and compaction.
     */
    public Entry readEntry(long offset) throws IOException {
        ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE);
        int bytesRead = channel.read(headerBuf, offset);
        if (bytesRead < HEADER_SIZE) {
            return null; // end of file or corrupt
        }
        headerBuf.flip();

        long timestamp = headerBuf.getLong();
        int keySize = headerBuf.getInt();
        int valueSize = headerBuf.getInt();

        ByteBuffer dataBuf = ByteBuffer.allocate(keySize + valueSize);
        channel.read(dataBuf, offset + HEADER_SIZE);
        dataBuf.flip();

        byte[] keyBytes = new byte[keySize];
        byte[] valueBytes = new byte[valueSize];
        dataBuf.get(keyBytes);
        dataBuf.get(valueBytes);

        return new Entry(
                timestamp,
                new String(keyBytes, "UTF-8"),
                new String(valueBytes, "UTF-8"),
                offset,
                keySize,
                valueSize
        );
    }

    /** Returns the size of this segment file in bytes. */
    public long size() {
        return currentOffset;
    }

    /** Returns the file ID of this segment. */
    public String getFileId() {
        return fileId;
    }

    public Path getFilePath() {
        return filePath;
    }

    public void close() throws IOException {
        channel.force(false); // fsync data only (not file metadata like timestamps)
        channel.close();
        raf.close();
    }

    /**
     * Iterates over all entries in this segment for recovery or compaction.
     */
    public void forEachEntry(EntryConsumer consumer) throws IOException {
        long offset = 0;
        while (offset < currentOffset) {
            Entry entry = readEntry(offset);
            if (entry == null) break;
            consumer.accept(entry);
            offset += HEADER_SIZE + entry.keySize() + entry.valueSize();
        }
    }

    @FunctionalInterface
    public interface EntryConsumer {
        void accept(Entry entry) throws IOException;
    }

    /** Represents a single key-value entry read from a segment. */
    public record Entry(long timestamp, String key, String value, long offset, int keySize, int valueSize) {}
}
