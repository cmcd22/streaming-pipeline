#!/usr/bin/env bash
set -e

echo "🐳 Starting Kafka infrastructure..."

docker compose up -d

echo "⏳ Waiting for Kafka to be ready..."
sleep 5

docker ps | grep kafka || {
  echo "❌ Kafka not running"
  exit 1
}

echo "✅ Kafka is running"
