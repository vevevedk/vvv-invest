# Deployment Guide

This guide covers the deployment of the vvv-invest system in a production environment.

## Prerequisites

- Ubuntu 20.04 LTS or later
- Python 3.8 or later
- PostgreSQL 12 or later
- Redis 6 or later (for Celery)
- Nginx (for dashboard)
- Systemd (for service management)

## System Setup

1. Update system packages:
```bash
sudo apt update
sudo apt upgrade -y
```

2. Install required system packages:
```bash
sudo apt install -y python3.8 python3.8-venv python3.8-dev postgresql postgresql-contrib redis-server nginx
```

3. Create system user:
```bash
sudo useradd -m -s /bin/bash vvv-invest
sudo usermod -aG sudo vvv-invest
```

## Application Setup

1. Clone repository:
```bash
sudo -u vvv-invest git clone https://github.com/your-org/vvv-invest.git /home/vvv-invest/app
cd /home/vvv-invest/app
```

2. Create and activate virtual environment:
```bash
sudo -u vvv-invest python3.8 -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
pip install -r requirements-test.txt
```

4. Set up environment variables:
```bash
sudo -u vvv-invest cp .env.example .env.prod
sudo -u vvv-invest nano .env.prod  # Edit with production values
```

## Database Setup

1. Create database and user:
```bash
sudo -u postgres psql
CREATE DATABASE vvv_invest;
CREATE USER vvv_invest WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE vvv_invest TO vvv_invest;
\q
```

2. Run migrations:
```bash
sudo -u vvv-invest python scripts/apply_migration.py
```

## Celery Configuration

1. Create Celery configuration file (`celery_config.py`):
```python
from celery import Celery
from celery.schedules import crontab
import os
from dotenv import load_dotenv

# Load environment variables
env_file = os.getenv("ENV_FILE", ".env")
load_dotenv(env_file, override=True)

# Initialize Celery
app = Celery('collectors',
             broker=os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0'),
             backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0'))

# Configure Celery
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='America/New_York',
    enable_utc=True,
    worker_max_tasks_per_child=1000,  # Restart worker after 1000 tasks
    worker_prefetch_multiplier=1,  # Process one task at a time
    task_acks_late=True,  # Only acknowledge task after completion
    task_reject_on_worker_lost=True,  # Requeue task if worker dies
)

# Configure periodic tasks
app.conf.beat_schedule = {
    'collect-dark-pool-trades': {
        'task': 'tasks.collect_dark_pool_trades',
        'schedule': crontab(minute='*/5'),  # Every 5 minutes
        'options': {'queue': 'dark_pool'}
    },
    'collect-news': {
        'task': 'tasks.collect_news',
        'schedule': crontab(minute='*/5'),  # Every 5 minutes
        'options': {'queue': 'news'}
    }
}
```

## Service Setup

### Collector Services

1. Create systemd service files:

**News Collector Worker Service** (`/etc/systemd/system/news-collector-worker.service`):
```ini
[Unit]
Description=News Collector Worker Service
After=network.target redis.service

[Service]
Type=simple
User=vvv-invest
Group=vvv-invest
WorkingDirectory=/home/vvv-invest/app
Environment=PYTHONPATH=/home/vvv-invest/app
Environment=UW_API_TOKEN=your_api_token
ExecStart=/home/vvv-invest/app/venv/bin/celery -A celery_config worker --loglevel=info -Q news -n news_worker@%h
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**News Collector Beat Service** (`/etc/systemd/system/news-collector-beat.service`):
```ini
[Unit]
Description=News Collector Beat Service
After=network.target redis.service

[Service]
Type=simple
User=vvv-invest
Group=vvv-invest
WorkingDirectory=/home/vvv-invest/app
Environment=PYTHONPATH=/home/vvv-invest/app
Environment=UW_API_TOKEN=your_api_token
ExecStart=/home/vvv-invest/app/venv/bin/celery -A celery_config beat --loglevel=info --schedule=/home/vvv-invest/app/celerybeat-news-schedule.db
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Dark Pool Collector Worker Service** (`/etc/systemd/system/darkpool-collector-worker.service`):
```ini
[Unit]
Description=Dark Pool Collector Worker Service
After=network.target redis.service

[Service]
Type=simple
User=vvv-invest
Group=vvv-invest
WorkingDirectory=/home/vvv-invest/app
Environment=PYTHONPATH=/home/vvv-invest/app
Environment=UW_API_TOKEN=your_api_token
ExecStart=/home/vvv-invest/app/venv/bin/celery -A celery_config worker --loglevel=info -Q dark_pool -n darkpool_worker@%h
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Dark Pool Collector Beat Service** (`/etc/systemd/system/darkpool-collector-beat.service`):
```ini
[Unit]
Description=Dark Pool Collector Beat Service
After=network.target redis.service

[Service]
Type=simple
User=vvv-invest
Group=vvv-invest
WorkingDirectory=/home/vvv-invest/app
Environment=PYTHONPATH=/home/vvv-invest/app
Environment=UW_API_TOKEN=your_api_token
ExecStart=/home/vvv-invest/app/venv/bin/celery -A celery_config beat --loglevel=info --schedule=/home/vvv-invest/app/celerybeat-darkpool-schedule.db
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

2. Enable and start services:
```bash
sudo systemctl daemon-reload
sudo systemctl enable news-collector-worker.service news-collector-beat.service darkpool-collector-worker.service darkpool-collector-beat.service
sudo systemctl start news-collector-worker.service news-collector-beat.service darkpool-collector-worker.service darkpool-collector-beat.service
```

## Dashboard Setup

1. Configure Nginx:
```bash
sudo nano /etc/nginx/sites-available/vvv-invest
```

Add configuration:
```nginx
server {
    listen 80;
    server_name your_domain.com;

    location / {
        proxy_pass http://localhost:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

2. Enable site and restart Nginx:
```bash
sudo ln -s /etc/nginx/sites-available/vvv-invest /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

## Logging Setup

1. Configure log rotation:
```bash
sudo nano /etc/logrotate.d/vvv-invest
```

Add configuration:
```conf
/home/vvv-invest/app/logs/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0640 vvv-invest vvv-invest
}
```

2. Create log directories:
```bash
sudo -u vvv-invest mkdir -p /home/vvv-invest/app/logs
```

## Monitoring

1. Check service status:
```bash
sudo systemctl status news-collector-worker.service
sudo systemctl status news-collector-beat.service
sudo systemctl status darkpool-collector-worker.service
sudo systemctl status darkpool-collector-beat.service
```

2. View logs:
```bash
sudo journalctl -u news-collector-worker.service -f
sudo journalctl -u news-collector-beat.service -f
sudo journalctl -u darkpool-collector-worker.service -f
sudo journalctl -u darkpool-collector-beat.service -f
```

## Backup

1. Set up database backups:
```bash
sudo -u vvv-invest crontab -e
```

Add backup job:
```bash
0 0 * * * pg_dump -U vvv_invest vvv_invest > /home/vvv-invest/app/backups/db_$(date +\%Y\%m\%d).sql
```

## Troubleshooting

### Common Issues

1. **Service Won't Start**
   - Check logs: `sudo journalctl -u service-name.service`
   - Verify environment variables
   - Check file permissions
   - Verify Redis connection

2. **No Data Collection**
   - Verify API token is set
   - Check collector logs
   - Verify database connection
   - Check if market is open (for dark pool collector)
   - Check Redis connection

3. **High Resource Usage**
   - Monitor system resources
   - Check for memory leaks
   - Verify rate limiting is working
   - Check Celery worker settings

### Validation Commands

1. **Validate data for last 24h:**
```bash
ENV_FILE=.env.prod python3 scripts/validate_data_last24h.py
```

2. **Backfill news headlines (last 24h):**
```bash
export UW_API_TOKEN=your_api_token && ENV_FILE=.env.prod python3 -c "from collectors.news_collector import NewsCollector; from datetime import datetime, timedelta; now=datetime.utcnow(); start=(now-timedelta(days=1)).strftime('%Y-%m-%d'); end=now.strftime('%Y-%m-%d'); NewsCollector().collect(start_date=start, end_date=end)"
```

3. **Backfill dark pool trades (last 24h):**
```bash
export UW_API_TOKEN=your_api_token && ENV_FILE=.env.prod python3 -c "from collectors.darkpool_collector import DarkPoolCollector; from datetime import datetime, timedelta; now=datetime.utcnow(); start=(now-timedelta(days=1)).strftime('%Y-%m-%d'); end=now.strftime('%Y-%m-%d'); DarkPoolCollector().collect_darkpool_trades(start_date=start, end_date=end, incremental=False)"
```

4. **Export last 24h to CSV:**
```bash
ENV_FILE=.env.prod python3 scripts/export_last24h.py
```

## Security Considerations

1. **Firewall Setup**
```bash
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable
```

2. **SSL/TLS Configuration**
- Install Certbot
- Obtain SSL certificate
- Configure Nginx for HTTPS

## Maintenance

1. **Regular Tasks**
   - Monitor disk space
   - Check log rotation
   - Verify backups
   - Update system packages
   - Monitor Redis memory usage
   - Check Celery worker health

2. **Database Maintenance**
   - Regular vacuum
   - Index maintenance
   - Data cleanup

## Support

For issues or questions:
1. Check the documentation
2. Review logs
3. Contact the development team 