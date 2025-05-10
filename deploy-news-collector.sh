#!/bin/bash

# Exit on error
set -e

# Stop existing services
echo "Stopping existing services..."
sudo systemctl stop news-collector-worker news-collector-beat

# Copy service files
echo "Copying service files..."
sudo cp news-collector-worker.service /etc/systemd/system/
sudo cp news-collector-beat.service /etc/systemd/system/
sudo cp news-collector.logrotate /etc/logrotate.d/

# Update service files for production paths
echo "Updating service files for production..."
sudo sed -i 's|/Users/iversen/Work/veveve/vvv-invest|/opt/darkpool_collector|g' /etc/systemd/system/news-collector-worker.service
sudo sed -i 's|/Users/iversen/Work/veveve/vvv-invest|/opt/darkpool_collector|g' /etc/systemd/system/news-collector-beat.service
sudo sed -i 's|/usr/local/bin|/opt/darkpool_collector/venv/bin|g' /etc/systemd/system/news-collector-worker.service
sudo sed -i 's|/usr/local/bin|/opt/darkpool_collector/venv/bin|g' /etc/systemd/system/news-collector-beat.service
sudo sed -i 's|iversen|avxz|g' /etc/systemd/system/news-collector-worker.service
sudo sed -i 's|iversen|avxz|g' /etc/systemd/system/news-collector-beat.service

# Update logrotate config for production paths
echo "Updating logrotate config..."
sudo sed -i 's|/Users/iversen/Work/veveve/vvv-invest|/opt/darkpool_collector|g' /etc/logrotate.d/news-collector
sudo sed -i 's|iversen|avxz|g' /etc/logrotate.d/news-collector

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