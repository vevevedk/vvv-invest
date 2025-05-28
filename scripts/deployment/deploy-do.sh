#!/bin/bash

# Exit on error
set -e

# Configuration
APP_NAME="darkpool_collector"
APP_DIR="/opt/${APP_NAME}"
VENV_DIR="${APP_DIR}/venv"
LOG_DIR="/var/log/${APP_NAME}"
CONFIG_DIR="/etc/${APP_NAME}"
DB_HOST="vvv-trading-db-do-user-21110609-0.i.db.ondigitalocean.com"
DB_PORT="25060"
DB_NAME="defaultdb"
DB_USER="doadmin"

# Update system packages
echo "Updating system packages..."
sudo apt-get update
sudo apt-get upgrade -y

# Install required packages
echo "Installing required packages..."
sudo apt-get install -y python3-venv python3-dev postgresql-client

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
After=network.target

[Service]
User=$USER
Group=$USER
WorkingDirectory=${APP_DIR}
Environment="PATH=${VENV_DIR}/bin"
Environment="DB_HOST=${DB_HOST}"
Environment="DB_PORT=${DB_PORT}"
Environment="DB_NAME=${DB_NAME}"
Environment="DB_USER=${DB_USER}"
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

# Verify database connection
echo "Verifying database connection..."
if ! psql "postgresql://${DB_USER}@${DB_HOST}:${DB_PORT}/${DB_NAME}?sslmode=require" -c "SELECT 1;" > /dev/null 2>&1; then
    echo "Warning: Could not connect to database. Please ensure:"
    echo "1. The database is running"
    echo "2. The connection details are correct"
    echo "3. The IP address is whitelisted in DO"
    echo "4. The SSL certificate is properly configured"
fi

echo "Deployment completed successfully!"
echo "To check service status: sudo systemctl status ${APP_NAME}"
echo "To view logs: tail -f ${LOG_DIR}/${APP_NAME}.log" 