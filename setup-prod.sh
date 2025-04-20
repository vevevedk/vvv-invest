#!/bin/bash

# Create collector user if it doesn't exist
sudo useradd -r -s /bin/false collector

# Create project directory
sudo mkdir -p /opt/vvv-invest
sudo chown collector:collector /opt/vvv-invest

# Clone repository
cd /opt/vvv-invest
sudo -u collector git clone https://github.com/vevevedk/vvv-invest.git .

# Set up Python virtual environment
sudo -u collector python3 -m venv venv
sudo -u collector venv/bin/pip install -e .

# Copy service file
sudo cp options-flow-collector.service /etc/systemd/system/

# Copy environment file
sudo cp .env /opt/vvv-invest/.env
sudo chown collector:collector /opt/vvv-invest/.env
sudo chmod 600 /opt/vvv-invest/.env

# Reload systemd and start service
sudo systemctl daemon-reload
sudo systemctl enable options-flow-collector
sudo systemctl start options-flow-collector

# Show status
sudo systemctl status options-flow-collector 