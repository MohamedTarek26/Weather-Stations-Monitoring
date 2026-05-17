# Weather Station Dockerfile
# Multi-stage build: compile with JDK 21, run with JRE 21

FROM eclipse-temurin:21-jdk AS builder
WORKDIR /build
COPY . .
RUN ./gradlew :weather-station:shadowJar --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=builder /build/weather-station/build/libs/weather-station.jar app.jar

# Default env vars (overridden by K8s deployment)
ENV STATION_ID=1
ENV KAFKA_BOOTSTRAP=kafka-service:9092
ENV TOPIC=weather-status

ENTRYPOINT ["java", "-jar", "app.jar"]
