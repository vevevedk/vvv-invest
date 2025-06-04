#!/bin/bash

# Exit on error
set -e

echo "Exporting Dark Pool data from production database..."

# Create export directory if it doesn't exist
mkdir -p exports/darkpool

# Activate virtual environment
source /opt/darkpool_collector/venv/bin/activate

# Run export script
cd /opt/darkpool_collector
python -m scripts.export_darkpool_data --hours 24 --output exports/darkpool

echo "Export completed! Data saved to exports/darkpool directory" 