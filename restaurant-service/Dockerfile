FROM ballerina/ballerina:2201.13.5 AS build
WORKDIR /app
COPY . .
USER root
RUN bal build

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/bin/restaurant_service.jar app.jar
COPY Config.toml .
EXPOSE 8082 9797
CMD ["java", "-jar", "app.jar"]
