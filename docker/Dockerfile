# syntax = docker/dockerfile:experimental

## BUILD image ##
FROM maven:3-eclipse-temurin-17 AS builder

# Install unzip for inspecting the extracted layers (optional)
RUN apt-get update && apt-get install -y unzip

WORKDIR /build

# Copy files
COPY ./src ./src

# COPY config files
COPY ./docker/parent_pom.xml ./pom.xml

RUN mvn clean package -DskipTests=true

# extract layers
RUN --mount=type=cache,id=maven-archival-storage-build,target=/build/archival-storage-build/build \
    java -Djarmode=layertools -jar ./target/archival-storage.jar extract

## RUN Image ##
FROM eclipse-temurin:17
WORKDIR /app

RUN mkdir /app/sample-storage-dir
RUN mkdir /app/workspace

COPY --from=builder /build/dependencies /app
COPY --from=builder /build/snapshot-dependencies /app
COPY --from=builder /build/spring-boot-loader /app
COPY --from=builder /build/application /app
CMD ["java", "org.springframework.boot.loader.launch.JarLauncher"]
