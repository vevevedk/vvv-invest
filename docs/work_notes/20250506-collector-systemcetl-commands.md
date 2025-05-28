# Dark Pool Collector Systemd Setup Commands

## 1. Install Service Files

```bash
# Copy service files to systemd directory
sudo cp darkpool-collector-worker.service /etc/systemd/system/
sudo cp darkpool-collector-beat.service /etc/systemd/system/

# Reload systemd to recognize new services
sudo systemctl daemon-reload
```

## 2. Enable and Start Services

```bash
# Enable services to start on boot
sudo systemctl enable darkpool-collector-worker
sudo systemctl enable darkpool-collector-beat

# Start the services
sudo systemctl start darkpool-collector-worker
sudo systemctl start darkpool-collector-beat
```

## 3. Check Service Status

```bash
# Check worker status
sudo systemctl status darkpool-collector-worker

# Check beat status
sudo systemctl status darkpool-collector-beat
```

## 4. Monitor Logs

```bash
# Monitor worker logs in real-time
sudo journalctl -u darkpool-collector-worker -f

# Monitor beat logs in real-time
sudo journalctl -u darkpool-collector-beat -f
```

## 5. Common Management Commands

```bash
# Stop services
sudo systemctl stop darkpool-collector-worker
sudo systemctl stop darkpool-collector-beat

# Restart services
sudo systemctl restart darkpool-collector-worker
sudo systemctl restart darkpool-collector-beat

# Disable services (prevent auto-start on boot)
sudo systemctl disable darkpool-collector-worker
sudo systemctl disable darkpool-collector-beat
```

## 6. Troubleshooting

If services fail to start, check:
1. Redis is running: `sudo systemctl status redis`
2. Service logs: `sudo journalctl -u darkpool-collector-worker -n 50`
3. Permissions: `ls -l /opt/darkpool_collector/venv/bin/celery`
4. Python environment: `source /opt/darkpool_collector/venv/bin/activate && which python`

## 7. Manual Testing (if needed)

```bash
# Activate virtual environment
cd /opt/darkpool_collector
source venv/bin/activate

# Test worker
celery -A celery_app.app worker --loglevel=info

# Test beat (in another terminal)
celery -A celery_app.app beat --loglevel=info
``` 