#!/usr/bin/env bash
set -e

echo "🚀 Starting streaming pipeline..."

# Start event generator
echo "▶️ Starting event generator..."
python producer/ride_event_generator.py &

GEN_PID=$!
echo "Generator PID: $GEN_PID"

# Start Bronze stream
echo "▶️ Starting Bronze stream..."
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  spark/bronze_stream.py &

BRONZE_PID=$!
echo "Bronze PID: $BRONZE_PID"

# Give Bronze time to initialise
sleep 10

# Start Silver stream
echo "▶️ Starting Silver stream..."
spark-submit spark/silver_stream.py &

SILVER_PID=$!
echo "Silver PID: $SILVER_PID"

echo ""
echo "✅ Pipeline running"
echo "Press Ctrl+C to stop foreground process"
wait
