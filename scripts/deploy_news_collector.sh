#!/bin/bash

# Stop existing services
sudo systemctl stop news-collector-worker
sudo systemctl stop news-collector-beat

# Copy service files
sudo cp config/systemd/news-collector-worker.service /etc/systemd/system/
sudo cp config/systemd/news-collector-beat.service /etc/systemd/system/

# Reload systemd
sudo systemctl daemon-reload

# Start services
sudo systemctl start news-collector-worker
sudo systemctl start news-collector-beat

# Enable services to start on boot
sudo systemctl enable news-collector-worker
sudo systemctl enable news-collector-beat

# Check status
echo "Checking service status..."
sudo systemctl status news-collector-worker
sudo systemctl status news-collector-beat 