#!/bin/bash

# Exit on error
set -e

# Stop existing services (ignore errors if they don't exist)
echo "Stopping existing services..."
sudo systemctl stop news-collector-worker news-collector-beat || true

# Copy service files
echo "Copying service files..."
sudo cp news-collector-worker.service /etc/systemd/system/
sudo cp news-collector-beat.service /etc/systemd/system/

# Copy logrotate config
echo "Copying logrotate config..."
sudo cp news-collector.logrotate /etc/logrotate.d/news-collector

# Update service files for production paths
echo "Updating service files for production..."
sudo sed -i 's|/Users/iversen/Work/veveve/vvv-invest|/opt/darkpool_collector|g' /etc/systemd/system/news-collector-worker.service
sudo sed -i 's|/Users/iversen/Work/veveve/vvv-invest|/opt/darkpool_collector|g' /etc/systemd/system/news-collector-beat.service
sudo sed -i 's|/usr/local/bin|/opt/darkpool_collector/venv/bin|g' /etc/systemd/system/news-collector-worker.service
sudo sed -i 's|/usr/local/bin|/opt/darkpool_collector/venv/bin|g' /etc/systemd/system/news-collector-beat.service
sudo sed -i 's|iversen|avxz|g' /etc/systemd/system/news-collector-worker.service
sudo sed -i 's|iversen|avxz|g' /etc/systemd/system/news-collector-beat.service

# Reload systemd
echo "Reloading systemd..."
sudo systemctl daemon-reload

# Start services
echo "Starting services..."
sudo systemctl start news-collector-worker
sudo systemctl start news-collector-beat
sudo systemctl enable news-collector-worker
sudo systemctl enable news-collector-beat

# Check status
echo "Checking service status..."
sudo systemctl status news-collector-worker
sudo systemctl status news-collector-beat

echo "Deployment completed successfully!" 