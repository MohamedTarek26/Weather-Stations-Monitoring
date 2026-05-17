package com.weathermon.central.archiver;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ParquetArchiver — Batched Parquet file writer for weather status archiving.
 *
 * Buffers incoming weather records in memory. When the buffer reaches BATCH_SIZE
 * (default 10,000), flushes all records to a Parquet file partitioned by station_id.
 *
 * File layout:
 *   archive/
 *   ├── station_id=1/
 *   │   ├── batch_0001.parquet
 *   │   └── batch_0002.parquet
 *   ├── station_id=2/
 *   │   └── batch_0001.parquet
 *   └── ...
 */
public class ParquetArchiver {

    private static final int DEFAULT_BATCH_SIZE = 10_000;

    private final String archiveDir;
    private final int batchSize;
    private final List<WeatherRecord> buffer;
    private final AtomicInteger fileCounter;

    // Avro schema matching our weather record
    private final Schema schema;

    public ParquetArchiver(String archiveDir) {
        this(archiveDir, DEFAULT_BATCH_SIZE);
    }

    public ParquetArchiver(String archiveDir, int batchSize) {
        this.archiveDir = archiveDir;
        this.batchSize = batchSize;
        this.buffer = new ArrayList<>(batchSize);
        this.fileCounter = new AtomicInteger(0);

        // Define the Avro schema for weather records
        this.schema = SchemaBuilder.record("WeatherStatus")
                .namespace("com.weathermon")
                .fields()
                .requiredLong("station_id")
                .requiredLong("s_no")
                .requiredString("battery_status")
                .requiredLong("status_timestamp")
                .requiredInt("humidity")
                .requiredInt("temperature")
                .requiredInt("wind_speed")
                .endRecord();

        System.out.printf("[ParquetArchiver] Ready — batch size: %d, dir: %s%n", batchSize, archiveDir);
    }

    /**
     * Adds a record to the buffer. When buffer is full, flushes to a Parquet file.
     */
    public synchronized void addRecord(WeatherRecord record) throws IOException {
        buffer.add(record);

        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    /**
     * Flushes all buffered records to Parquet files, partitioned by station_id.
     * Each station gets its own subdirectory.
     */
    public synchronized void flush() throws IOException {
        if (buffer.isEmpty()) return;

        int batchNum = fileCounter.incrementAndGet();

        // Group records by station_id for partitioning
        var byStation = new java.util.LinkedHashMap<Long, List<WeatherRecord>>();
        for (WeatherRecord r : buffer) {
            byStation.computeIfAbsent(r.getStationId(), k -> new ArrayList<>()).add(r);
        }

        for (var entry : byStation.entrySet()) {
            long stationId = entry.getKey();
            List<WeatherRecord> records = entry.getValue();

            // Create partition directory
            String partitionDir = archiveDir + "/station_" + stationId;
            Files.createDirectories(java.nio.file.Path.of(partitionDir));

            String fileName = String.format("%s/batch_%04d.parquet", partitionDir, batchNum);
            writeParquetFile(fileName, records);
        }

        System.out.printf("[ParquetArchiver] Flushed batch %d — %d records across %d stations%n",
                batchNum, buffer.size(), byStation.size());

        buffer.clear();
    }

    /**
     * Writes a list of records to a single Parquet file.
     */
    private void writeParquetFile(String filePath, List<WeatherRecord> records) throws IOException {
        // Delete existing file if present (Parquet writer doesn't overwrite)
        java.nio.file.Path path = java.nio.file.Path.of(filePath);
        Files.deleteIfExists(path);

        Configuration conf = new Configuration();
        Path hadoopPath = new Path(filePath);

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            for (WeatherRecord record : records) {
                GenericRecord avroRecord = new GenericData.Record(schema);
                avroRecord.put("station_id", record.getStationId());
                avroRecord.put("s_no", record.getSequenceNumber());
                avroRecord.put("battery_status", record.getBatteryStatus());
                avroRecord.put("status_timestamp", record.getStatusTimestamp());
                avroRecord.put("humidity", record.getHumidity());
                avroRecord.put("temperature", record.getTemperature());
                avroRecord.put("wind_speed", record.getWindSpeed());

                writer.write(avroRecord);
            }
        }
    }

    /** Returns the number of records currently buffered (not yet flushed). */
    public int getBufferedCount() {
        return buffer.size();
    }

    /** Returns the Avro schema used for Parquet files. */
    public Schema getSchema() {
        return schema;
    }
}
