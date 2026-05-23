FROM maven:3.9-eclipse-temurin-11 AS builder

WORKDIR /app
COPY pom.xml .
# Cache dependencies
RUN mvn dependency:go-offline -B

COPY src/ src/
RUN mvn compile -B -q

# Generate the savepoint
RUN mvn exec:java -Dexec.mainClass="com.test.explorer.SavepointGenerator" -B \
    || echo "Savepoint generation completed (job cancelled after savepoint)"

# Copy savepoint to a known location
RUN mkdir -p /savepoint && \
    cp -r target/savepoint-test/savepoint-*/ /savepoint/test-savepoint/ 2>/dev/null || \
    echo "No savepoint directory found, checking target..." && \
    ls -la target/savepoint-test/ 2>/dev/null || true

# --- Lightweight output image ---
FROM alpine:3.19
COPY --from=builder /savepoint/ /savepoint/
VOLUME /output
CMD ["sh", "-c", "cp -r /savepoint/* /output/ 2>/dev/null && echo 'Savepoint copied to /output' && ls -la /output/"]
