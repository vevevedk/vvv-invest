#!/bin/bash

# Exit on error
set -e

# Configuration
APP_NAME="darkpool_collector"
APP_DIR="/opt/${APP_NAME}"
VENV_DIR="${APP_DIR}/venv"
LOG_DIR="/var/log/${APP_NAME}"
CONFIG_DIR="/etc/${APP_NAME}"

# Create necessary directories
echo "Creating directories..."
sudo mkdir -p ${APP_DIR} ${LOG_DIR} ${CONFIG_DIR}
sudo chown -R $USER:$USER ${APP_DIR} ${LOG_DIR} ${CONFIG_DIR}

# Set up Python virtual environment
echo "Setting up Python virtual environment..."
python3 -m venv ${VENV_DIR}
source ${VENV_DIR}/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Copy application files
echo "Copying application files..."
cp -r flow_analysis ${APP_DIR}/
cp requirements.txt ${APP_DIR}/
cp setup.py ${APP_DIR}/

# Set up logging
echo "Setting up logging..."
sudo touch ${LOG_DIR}/${APP_NAME}.log
sudo chown -R $USER:$USER ${LOG_DIR}

# Create systemd service
echo "Creating systemd service..."
cat > /tmp/${APP_NAME}.service << EOF
[Unit]
Description=Dark Pool Trade Collector
After=network.target postgresql.service

[Service]
User=$USER
Group=$USER
WorkingDirectory=${APP_DIR}
Environment="PATH=${VENV_DIR}/bin"
ExecStart=${VENV_DIR}/bin/python -m flow_analysis.scripts.darkpool_collector
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Install and enable service
echo "Installing and enabling service..."
sudo mv /tmp/${APP_NAME}.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable ${APP_NAME}
sudo systemctl start ${APP_NAME}

# Set up log rotation
echo "Setting up log rotation..."
cat > /tmp/${APP_NAME}-logrotate << EOF
${LOG_DIR}/*.log {
    daily
    missingok
    rotate 14
    compress
    delaycompress
    notifempty
    create 0640 $USER $USER
}
EOF

sudo mv /tmp/${APP_NAME}-logrotate /etc/logrotate.d/${APP_NAME}
sudo chown root:root /etc/logrotate.d/${APP_NAME}

echo "Deployment completed successfully!"
echo "To check service status: sudo systemctl status ${APP_NAME}"
echo "To view logs: tail -f ${LOG_DIR}/${APP_NAME}.log" 