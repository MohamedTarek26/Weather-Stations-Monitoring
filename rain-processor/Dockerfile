# Rain Processor Dockerfile

FROM eclipse-temurin:21-jdk AS builder
WORKDIR /build
COPY . .
RUN ./gradlew :rain-processor:shadowJar --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=builder /build/rain-processor/build/libs/rain-processor.jar app.jar

ENV KAFKA_BOOTSTRAP=kafka-service:9092
ENV INPUT_TOPIC=weather-status
ENV OUTPUT_TOPIC=rain-alerts

ENTRYPOINT ["java", "-jar", "app.jar"]
