#!/bin/bash

# Exit on error
set -e

# Update system packages
echo "Updating system packages..."
sudo apt-get update
sudo apt-get upgrade -y

# Install required packages
echo "Installing required packages..."
sudo apt-get install -y python3-pip python3-dev postgresql postgresql-contrib

# Install Python packages
echo "Installing Python packages..."
pip3 install pandas sqlalchemy psycopg2-binary requests python-dotenv pytz

# Create project directory
echo "Setting up project directory..."
PROJECT_DIR="/opt/darkpool_collector"
sudo mkdir -p $PROJECT_DIR
sudo chown $USER:$USER $PROJECT_DIR

# Copy project files
echo "Copying project files..."
cp -r flow_analysis $PROJECT_DIR/
cp .env $PROJECT_DIR/

# Set up logging directory
echo "Setting up logging..."
sudo mkdir -p /var/log/darkpool_collector
sudo chown $USER:$USER /var/log/darkpool_collector

# Create cron job
echo "Setting up cron job..."
CRON_JOB="*/5 * * * * cd $PROJECT_DIR && python3 flow_analysis/scripts/collect_darkpool_trades.py >> /var/log/darkpool_collector/cron.log 2>&1"
(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

echo "Deployment completed successfully!" 