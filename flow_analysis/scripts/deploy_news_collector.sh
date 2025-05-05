#!/bin/bash

# News Collector Deployment Script
# This script sets up the news collector as a systemd service

# Configuration
SERVICE_NAME="news-collector"
SERVICE_USER="collector"
SERVICE_GROUP="collector"
INSTALL_DIR="/opt/news-collector"
VENV_DIR="$INSTALL_DIR/venv"
LOG_DIR="/var/log/news-collector"
CONFIG_DIR="/etc/news-collector"

# Create service user and group if they don't exist
if ! id "$SERVICE_USER" &>/dev/null; then
    useradd -r -s /bin/false "$SERVICE_USER"
fi

# Create directories
mkdir -p "$INSTALL_DIR" "$LOG_DIR" "$CONFIG_DIR"
chown -R "$SERVICE_USER:$SERVICE_GROUP" "$INSTALL_DIR" "$LOG_DIR" "$CONFIG_DIR"

# Create Python virtual environment
python3 -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"

# Install dependencies
pip install -r "$INSTALL_DIR/requirements.txt"

# Create systemd service file
cat > "/etc/systemd/system/$SERVICE_NAME.service" << EOF
[Unit]
Description=News Headlines Collector
After=network.target postgresql.service

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_GROUP
WorkingDirectory=$INSTALL_DIR
Environment="PATH=$VENV_DIR/bin"
ExecStart=$VENV_DIR/bin/python $INSTALL_DIR/flow_analysis/scripts/news_collector.py
Restart=always
RestartSec=60
StandardOutput=append:$LOG_DIR/output.log
StandardError=append:$LOG_DIR/error.log

[Install]
WantedBy=multi-user.target
EOF

# Create logrotate configuration
cat > "/etc/logrotate.d/$SERVICE_NAME" << EOF
$LOG_DIR/*.log {
    daily
    missingok
    rotate 7
    compress
    delaycompress
    notifempty
    create 0640 $SERVICE_USER $SERVICE_GROUP
    sharedscripts
    postrotate
        systemctl reload $SERVICE_NAME >/dev/null 2>&1 || true
    endscript
}
EOF

# Set permissions
chown -R "$SERVICE_USER:$SERVICE_GROUP" "$LOG_DIR"
chmod 750 "$LOG_DIR"

# Reload systemd and enable service
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"

# Start the service
systemctl start "$SERVICE_NAME"

# Check service status
systemctl status "$SERVICE_NAME"

# Create monitoring script
cat > "$INSTALL_DIR/monitor_collector.sh" << 'EOF'
#!/bin/bash

# Monitor news collector service
SERVICE_NAME="news-collector"
LOG_FILE="/var/log/news-collector/output.log"
ERROR_FILE="/var/log/news-collector/error.log"
ALERT_EMAIL="admin@example.com"

# Check service status
if ! systemctl is-active --quiet "$SERVICE_NAME"; then
    echo "Service $SERVICE_NAME is not running!" | mail -s "News Collector Alert" "$ALERT_EMAIL"
    systemctl restart "$SERVICE_NAME"
fi

# Check for errors in log files
if grep -i "error" "$ERROR_FILE" | tail -n 1 > /tmp/error_check; then
    if [ -s /tmp/error_check ]; then
        cat /tmp/error_check | mail -s "News Collector Error" "$ALERT_EMAIL"
    fi
fi

# Check for successful collection
if ! grep -q "Collection cycle completed" "$LOG_FILE" | tail -n 1; then
    echo "No successful collection in last cycle" | mail -s "News Collector Warning" "$ALERT_EMAIL"
fi
EOF

# Make monitoring script executable
chmod +x "$INSTALL_DIR/monitor_collector.sh"

# Add monitoring to crontab
(crontab -l 2>/dev/null; echo "*/5 * * * * $INSTALL_DIR/monitor_collector.sh") | crontab -

echo "News collector deployment complete!"
echo "Service name: $SERVICE_NAME"
echo "Install directory: $INSTALL_DIR"
echo "Log directory: $LOG_DIR"
echo "Configuration directory: $CONFIG_DIR" 