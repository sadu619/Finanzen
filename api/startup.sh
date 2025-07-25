#!/bin/bash

# SAP Integration API Startup Script for Azure Web App (Linux)
echo "🚀 Starting SAP Integration API..."

# Set environment variables if not already set
export PYTHONPATH="${PYTHONPATH}:/home/site/wwwroot"
export WEBSITES_PORT="${WEBSITES_PORT:-8000}"

# Change to application directory
cd /home/site/wwwroot

# Display startup information
echo "📍 Current directory: $(pwd)"
echo "🐍 Python version: $(python3 --version)"
echo "📂 Available files: $(ls -la)"

# Check if required files exist
if [ ! -f "app.py" ]; then
    echo "❌ app.py not found!"
    exit 1
fi

if [ ! -f "requirements.txt" ]; then
    echo "❌ requirements.txt not found!"
    exit 1
fi

# Install dependencies if needed (should already be installed during deployment)
echo "📦 Checking dependencies..."
pip3 install --no-cache-dir -r requirements.txt

# Check if processing logic is available
if [ -f "msp_sap_integration_fixed.py" ]; then
    echo "✅ Processing logic found: msp_sap_integration_fixed.py"
else
    echo "⚠️  Processing logic not found in current directory"
fi

# Start the application with Gunicorn
echo "🌐 Starting Gunicorn server on port $WEBSITES_PORT..."
exec gunicorn \
    --bind=0.0.0.0:$WEBSITES_PORT \
    --timeout 600 \
    --workers 1 \
    --worker-class sync \
    --preload \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    app:app