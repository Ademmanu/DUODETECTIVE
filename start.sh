#!/usr/bin/env bash
# Start script for Render stable deployment.
# Render will run this script. Make sure it's executable (chmod +x start.sh).
set -euo pipefail

# Optional: allow overriding PORT (used by Flask)
export PORT=${PORT:-5000}

echo "Starting Duplicate Monitor service..."
python monitor.py