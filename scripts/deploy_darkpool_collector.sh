#!/bin/bash

# Exit on error
set -e

echo "Deploying Dark Pool Collector..."

# Create necessary directories
sudo mkdir -p /opt/darkpool_collector
sudo mkdir -p /var/lib/celery/beat

# Copy service files
sudo cp config/systemd/darkpool-collector-worker.service /etc/systemd/system/
sudo cp config/systemd/darkpool-collector-beat.service /etc/systemd/system/

# Copy application files
sudo cp -r collectors config scripts flow_analysis /opt/darkpool_collector/

# Create and activate virtual environment
sudo python3 -m venv /opt/darkpool_collector/venv
sudo /opt/darkpool_collector/venv/bin/pip install -r requirements.txt

# Set permissions
sudo chown -R avxz:avxz /opt/darkpool_collector
sudo chown -R avxz:avxz /var/lib/celery/beat

# Reload systemd
sudo systemctl daemon-reload

# Enable and start services
sudo systemctl enable darkpool-collector-worker
sudo systemctl enable darkpool-collector-beat
sudo systemctl start darkpool-collector-worker
sudo systemctl start darkpool-collector-beat

echo "Dark Pool Collector deployment completed!"
echo "Checking service status..."
sudo systemctl status darkpool-collector-worker
sudo systemctl status darkpool-collector-beat 