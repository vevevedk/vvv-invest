#!/bin/bash

# Configuration
INSTALL_DIR="/opt/darkpool_collector"
LOG_DIR="/var/log/flow_alerts_collector"
CONFIG_DIR="/etc/darkpool_collector"
SERVICE_NAME="flow-alerts-collector"

# Create necessary directories
sudo mkdir -p "$INSTALL_DIR"
sudo mkdir -p "$LOG_DIR"
sudo mkdir -p "$CONFIG_DIR"

# Copy service file
sudo cp systemd/flow-alerts-collector.service /etc/systemd/system/

# Set up Python virtual environment
sudo python3 -m venv "$INSTALL_DIR/venv"
sudo "$INSTALL_DIR/venv/bin/pip" install --upgrade pip
sudo "$INSTALL_DIR/venv/bin/pip" install -r requirements.txt

# Copy configuration files
sudo cp .env.prod "$CONFIG_DIR/"
sudo cp flow_analysis/config/*.py "$CONFIG_DIR/"

# Set up logging
sudo touch "$LOG_DIR/output.log"
sudo touch "$LOG_DIR/error.log"
sudo chown -R darkpool_collector:darkpool_collector "$LOG_DIR"

# Set permissions
sudo chown -R darkpool_collector:darkpool_collector "$INSTALL_DIR"
sudo chown -R darkpool_collector:darkpool_collector "$CONFIG_DIR"

# Reload systemd
sudo systemctl daemon-reload

# Enable and start service
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl start "$SERVICE_NAME"

# Create monitoring script
cat > "$INSTALL_DIR/monitor_flow_alerts.sh" << 'EOF'
#!/bin/bash

# Monitor flow alerts collector service
SERVICE_NAME="flow-alerts-collector"
LOG_FILE="/var/log/flow_alerts_collector/output.log"
ERROR_FILE="/var/log/flow_alerts_collector/error.log"
ALERT_EMAIL="admin@example.com"

# Check service status
if ! systemctl is-active --quiet "$SERVICE_NAME"; then
    echo "Service $SERVICE_NAME is not running!" | mail -s "Flow Alerts Collector Alert" "$ALERT_EMAIL"
    systemctl restart "$SERVICE_NAME"
fi

# Check for errors in log files
if grep -i "error" "$ERROR_FILE" | tail -n 1 > /tmp/error_check; then
    if [ -s /tmp/error_check ]; then
        cat /tmp/error_check | mail -s "Flow Alerts Collector Error" "$ALERT_EMAIL"
    fi
fi

# Check for successful collection
if ! grep -q "Successfully saved" "$LOG_FILE" | tail -n 1; then
    echo "No successful collection in last cycle" | mail -s "Flow Alerts Collector Warning" "$ALERT_EMAIL"
fi
EOF

# Make monitoring script executable
chmod +x "$INSTALL_DIR/monitor_flow_alerts.sh"

# Add monitoring to crontab
(crontab -l 2>/dev/null; echo "*/5 * * * * $INSTALL_DIR/monitor_flow_alerts.sh") | crontab -

echo "Flow alerts collector deployment complete!"
echo "Service name: $SERVICE_NAME"
echo "Install directory: $INSTALL_DIR"
echo "Log directory: $LOG_DIR"
echo "Configuration directory: $CONFIG_DIR" 