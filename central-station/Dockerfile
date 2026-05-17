# Central Station Dockerfile
# Multi-stage build: compile with JDK 21, run with JDK 21 (need JDK for JFR profiling)

FROM eclipse-temurin:21-jdk AS builder
WORKDIR /build
COPY . .
RUN ./gradlew :central-station:shadowJar --no-daemon

FROM eclipse-temurin:21-jdk
WORKDIR /app
COPY --from=builder /build/central-station/build/libs/central-station.jar app.jar

# Create data directories
RUN mkdir -p /app/data/bitcask /app/data/archive

# Default env vars
ENV KAFKA_BOOTSTRAP=kafka-service:9092
ENV TOPIC=weather-status
ENV BITCASK_DIR=/app/data/bitcask
ENV ARCHIVE_DIR=/app/data/archive
ENV API_PORT=8080

# JFR flags for Module G profiling
ENV JAVA_OPTS=""

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
