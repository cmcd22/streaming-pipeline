#!/usr/bin/env bash
set -e

echo "🔧 Setting up environment..."

# Java 11 (Spark compatible)
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
export PATH="$JAVA_HOME/bin:$PATH"

# Activate Python venv
if [ ! -d ".venv" ]; then
  echo "❌ .venv not found. Please create it first."
  exit 1
fi

source .venv/bin/activate

# Force Spark to use venv Python
export PYSPARK_PYTHON=$(which python)
export PYSPARK_DRIVER_PYTHON=$(which python)

echo "✅ Environment ready"
echo "Java: $(java -version 2>&1 | head -n 1)"
echo "Python: $(python --version)"
