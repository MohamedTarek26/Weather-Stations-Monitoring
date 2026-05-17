package com.weathermon.central.archiver;

/**
 * WeatherRecord — Plain data object representing one weather status message.
 * Used by ParquetArchiver to buffer records before flushing to Parquet files.
 */
public class WeatherRecord {

    private long stationId;
    private long sequenceNumber;
    private String batteryStatus;
    private long statusTimestamp;
    private int humidity;
    private int temperature;
    private int windSpeed;

    public WeatherRecord() {}

    public WeatherRecord(long stationId, long sequenceNumber, String batteryStatus,
                         long statusTimestamp, int humidity, int temperature, int windSpeed) {
        this.stationId = stationId;
        this.sequenceNumber = sequenceNumber;
        this.batteryStatus = batteryStatus;
        this.statusTimestamp = statusTimestamp;
        this.humidity = humidity;
        this.temperature = temperature;
        this.windSpeed = windSpeed;
    }

    // ─── Getters ─────────────────────────────────────────────────

    public long getStationId()       { return stationId; }
    public long getSequenceNumber()  { return sequenceNumber; }
    public String getBatteryStatus() { return batteryStatus; }
    public long getStatusTimestamp()  { return statusTimestamp; }
    public int getHumidity()         { return humidity; }
    public int getTemperature()      { return temperature; }
    public int getWindSpeed()        { return windSpeed; }

    // ─── Setters ─────────────────────────────────────────────────

    public void setStationId(long stationId)            { this.stationId = stationId; }
    public void setSequenceNumber(long sequenceNumber)   { this.sequenceNumber = sequenceNumber; }
    public void setBatteryStatus(String batteryStatus)   { this.batteryStatus = batteryStatus; }
    public void setStatusTimestamp(long statusTimestamp)  { this.statusTimestamp = statusTimestamp; }
    public void setHumidity(int humidity)                { this.humidity = humidity; }
    public void setTemperature(int temperature)          { this.temperature = temperature; }
    public void setWindSpeed(int windSpeed)              { this.windSpeed = windSpeed; }

    @Override
    public String toString() {
        return String.format("WeatherRecord{station=%d, s_no=%d, battery=%s, humidity=%d, temp=%d, wind=%d}",
                stationId, sequenceNumber, batteryStatus, humidity, temperature, windSpeed);
    }
}
