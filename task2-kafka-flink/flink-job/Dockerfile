FROM maven:3.9.16-eclipse-temurin-17 AS build

WORKDIR /build

COPY pom.xml .
RUN mvn --batch-mode --no-transfer-progress dependency:go-offline

COPY src ./src
RUN mvn --batch-mode --no-transfer-progress clean package -DskipTests

FROM busybox:1.37.0

COPY --from=build /build/target/traffic-window-job.jar /traffic-window-job.jar

CMD ["sh", "-c", "cp /traffic-window-job.jar /output/traffic-window-job.jar"]
