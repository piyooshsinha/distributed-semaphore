# Build context: semaphore-simulator/ (only target/semaphore-simulator-exec.jar is sent, see .dockerignore).
# The jar is small and has no Spring at runtime, so it is not split into layers.
FROM eclipse-temurin:25-jre
RUN groupadd --system --gid 10001 app && useradd --system --uid 10001 --gid app --no-create-home app
WORKDIR /app
COPY target/semaphore-simulator-exec.jar app.jar
USER app
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
