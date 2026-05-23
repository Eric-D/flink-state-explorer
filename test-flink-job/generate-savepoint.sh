#!/bin/bash
# Generate a Flink savepoint and copy it to the specified output directory.
# Usage: ./generate-savepoint.sh [output_dir]
#
# Default output: ./target/savepoint-test/

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTPUT_DIR="${1:-$SCRIPT_DIR/target/savepoint-test}"

echo "=== Flink Savepoint Generator ==="
echo "Output directory: $OUTPUT_DIR"

cd "$SCRIPT_DIR"

echo ""
echo "--- Building project ---"
mvn compile -B -q

echo ""
echo "--- Generating savepoint ---"
mvn exec:java -Dexec.mainClass="com.test.explorer.SavepointGenerator" -B 2>&1 || true

echo ""
echo "--- Savepoint files ---"
SAVEPOINT_DIR=$(find target/savepoint-test -maxdepth 1 -type d -name 'savepoint-*' 2>/dev/null | head -1)

if [ -z "$SAVEPOINT_DIR" ]; then
    echo "ERROR: No savepoint directory found!"
    ls -la target/savepoint-test/ 2>/dev/null || echo "target/savepoint-test/ does not exist"
    exit 1
fi

echo "Found: $SAVEPOINT_DIR"
ls -la "$SAVEPOINT_DIR"

# Copy to output if different from source
if [ "$OUTPUT_DIR" != "$SCRIPT_DIR/target/savepoint-test" ]; then
    mkdir -p "$OUTPUT_DIR"
    cp -r "$SAVEPOINT_DIR" "$OUTPUT_DIR/"
    echo ""
    echo "Copied to: $OUTPUT_DIR/$(basename $SAVEPOINT_DIR)"
fi

echo ""
echo "=== Done ==="
echo "Explore with: cargo run -- $SAVEPOINT_DIR --no-cache"
