#!/bin/bash
set -e

FLINK_REST="http://jobmanager:8081"
OUTPUT_DIR="/savepoints"

echo "=== Flink Savepoint Generator (Cluster Mode) ==="

# Wait for Flink cluster to be ready
echo "Waiting for Flink cluster..."
for i in $(seq 1 60); do
    if curl -sf "$FLINK_REST/overview" > /dev/null 2>&1; then
        echo "Flink cluster is ready!"
        break
    fi
    echo "  Attempt $i/60..."
    sleep 2
done

curl -sf "$FLINK_REST/overview" || { echo "ERROR: Flink cluster not reachable"; exit 1; }

# Build the uber-jar
echo ""
echo "--- Building job jar ---"
cd /app
mvn package -B -q -DskipTests

JAR_PATH=$(find target -name "flink-savepoint-generator-*.jar" -not -name "*original*" | head -1)
echo "Jar: $JAR_PATH"

# Upload jar
echo ""
echo "--- Uploading jar ---"
UPLOAD_RESPONSE=$(curl -sf -X POST "$FLINK_REST/jars/upload" \
    -H "Content-Type: multipart/form-data" \
    -F "jarfile=@$JAR_PATH;type=application/java-archive")
JAR_ID=$(echo "$UPLOAD_RESPONSE" | grep -o '"filename":"[^"]*"' | head -1 | cut -d'"' -f4 | xargs basename)
echo "Uploaded jar ID: $JAR_ID"

# Submit job
echo ""
echo "--- Submitting job ---"
RUN_RESPONSE=$(curl -sf -X POST "$FLINK_REST/jars/$JAR_ID/run" \
    -H "Content-Type: application/json" \
    -d '{"entryClass":"com.test.explorer.ClusterJob","parallelism":2}')
JOB_ID=$(echo "$RUN_RESPONSE" | grep -o '"jobid":"[^"]*"' | cut -d'"' -f4)
echo "Job ID: $JOB_ID"

# Wait for RUNNING state
echo ""
echo "--- Waiting for job to be RUNNING ---"
for i in $(seq 1 30); do
    STATUS=$(curl -sf "$FLINK_REST/jobs/$JOB_ID" | grep -o '"state":"[^"]*"' | cut -d'"' -f4)
    echo "  Status: $STATUS"
    if [ "$STATUS" = "RUNNING" ]; then
        break
    fi
    if [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELED" ] || [ "$STATUS" = "FINISHED" ]; then
        echo "ERROR: Job reached terminal state: $STATUS"
        curl -sf "$FLINK_REST/jobs/$JOB_ID/exceptions" 2>/dev/null | head -20
        exit 1
    fi
    sleep 2
done

# Wait for state to be populated
echo ""
echo "Waiting 5s for state population..."
sleep 5

# Trigger canonical savepoint
echo ""
echo "--- Triggering savepoint ---"
TRIGGER_RESPONSE=$(curl -sf -X POST "$FLINK_REST/jobs/$JOB_ID/savepoints" \
    -H "Content-Type: application/json" \
    -d "{\"cancel-job\":true,\"formatType\":\"CANONICAL\",\"target-directory\":\"file://$OUTPUT_DIR\"}")
TRIGGER_ID=$(echo "$TRIGGER_RESPONSE" | grep -o '"request-id":"[^"]*"' | cut -d'"' -f4)
echo "Trigger ID: $TRIGGER_ID"

# Poll savepoint status
echo "Waiting for savepoint completion..."
for i in $(seq 1 60); do
    SP_STATUS=$(curl -sf "$FLINK_REST/jobs/$JOB_ID/savepoints/$TRIGGER_ID")
    SP_STATE=$(echo "$SP_STATUS" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)

    if [ "$SP_STATE" = "COMPLETED" ]; then
        SP_PATH=$(echo "$SP_STATUS" | grep -o '"location":"[^"]*"' | cut -d'"' -f4)
        echo "Savepoint completed: $SP_PATH"
        break
    elif [ "$SP_STATE" = "FAILED" ]; then
        echo "ERROR: Savepoint failed"
        echo "$SP_STATUS"
        exit 1
    fi
    echo "  Status: $SP_STATE ($i/60)"
    sleep 2
done

# List savepoint files
echo ""
echo "--- Savepoint contents ---"
SAVEPOINT_DIR=$(find "$OUTPUT_DIR" -maxdepth 1 -type d -name 'savepoint-*' | head -1)
if [ -n "$SAVEPOINT_DIR" ]; then
    ls -la "$SAVEPOINT_DIR"
    echo ""
    echo "Savepoint: $SAVEPOINT_DIR"

    # Copy to app output
    mkdir -p /app/output
    cp -r "$SAVEPOINT_DIR" /app/output/
    echo "Copied to /app/output/$(basename $SAVEPOINT_DIR)"
else
    echo "WARNING: No savepoint directory found in $OUTPUT_DIR"
    ls -la "$OUTPUT_DIR" 2>/dev/null
fi

echo ""
echo "=== Done ==="
