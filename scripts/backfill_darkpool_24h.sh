#!/bin/bash

# Exit on error
set -e

echo "Starting Dark Pool Collector backfill for last 24 hours..."

# Activate virtual environment
source /opt/darkpool_collector/venv/bin/activate

# Run backfill
cd /opt/darkpool_collector
python -m collectors.darkpool.darkpool_collector_backfill --hours 24

echo "Backfill completed!" 