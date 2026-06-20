FROM eclipse-temurin:21-jdk AS build

WORKDIR /workspace
COPY gradle ./gradle
COPY gradlew gradle.properties settings.gradle.kts ./
COPY lib/build.gradle.kts ./lib/build.gradle.kts
COPY lib/src ./lib/src
RUN chmod +x ./gradlew
RUN ./gradlew :lib:installDist --no-daemon

FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build /workspace/lib/build/install/lib ./
EXPOSE 50051
ENTRYPOINT ["/app/bin/lib"]
