#!/bin/bash

# Exit on error
set -e

# Pull latest changes
echo "Pulling latest changes from git..."
git pull origin main

# Set up database tables
echo "Setting up database tables..."
psql -d trading_data -f flow_analysis/scripts/create_news_tables.sql

# Set up cron job if it doesn't exist
echo "Setting up cron job..."
CRON_JOB="*/5 * * * * cd /opt/vvv-invest && /opt/vvv-invest/venv/bin/python3 flow_analysis/scripts/news_collector.py >> /var/log/news_collector/cron.log 2>&1"

# Check if cron job already exists
if ! crontab -l | grep -q "news_collector.py"; then
    (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
    echo "Cron job added successfully"
else
    echo "Cron job already exists"
fi

# Create log directory if it doesn't exist
echo "Setting up logging..."
sudo mkdir -p /var/log/news_collector
sudo chown collector:collector /var/log/news_collector

# Set up logrotate
echo "Setting up logrotate..."
sudo cp news-collector.logrotate /etc/logrotate.d/news_collector
sudo chown root:root /etc/logrotate.d/news_collector
sudo chmod 644 /etc/logrotate.d/news_collector

# Stop existing services
echo "Stopping existing services..."
sudo systemctl stop news-collector-worker news-collector-beat

# Copy service files
echo "Copying service files..."
sudo cp news-collector-worker.service /etc/systemd/system/
sudo cp news-collector-beat.service /etc/systemd/system/
sudo cp news-collector.logrotate /etc/logrotate.d/

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