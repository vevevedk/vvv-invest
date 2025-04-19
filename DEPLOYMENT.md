# Dark Pool Trade Collector Deployment Guide

## Prerequisites
- Digital Ocean droplet (Ubuntu 20.04 or later recommended)
- SSH access to the droplet
- PostgreSQL database (can be on the same droplet or a separate managed database)

## Deployment Steps

1. **Connect to your droplet**
   ```bash
   ssh root@your_droplet_ip
   ```

2. **Create a non-root user (recommended)**
   ```bash
   adduser deployer
   usermod -aG sudo deployer
   ```

3. **Set up SSH for the new user**
   ```bash
   mkdir -p /home/deployer/.ssh
   cp ~/.ssh/authorized_keys /home/deployer/.ssh/
   chown -R deployer:deployer /home/deployer/.ssh
   chmod 700 /home/deployer/.ssh
   chmod 600 /home/deployer/.ssh/authorized_keys
   ```

4. **Transfer project files**
   From your local machine:
   ```bash
   scp -r ./* deployer@your_droplet_ip:/tmp/darkpool_collector
   ```

5. **Run the deployment script**
   On the droplet:
   ```bash
   cd /tmp/darkpool_collector
   chmod +x deploy.sh
   ./deploy.sh
   ```

6. **Verify the setup**
   - Check the cron job:
     ```bash
     crontab -l
     ```
   - Check the logs:
     ```bash
     tail -f /var/log/darkpool_collector/cron.log
     ```

## Configuration

1. **Environment Variables**
   Make sure your `.env` file contains:
   - API_KEY: Your Unusual Whales API key
   - DB_CONFIG: Database connection details

2. **Database Setup**
   If using a local PostgreSQL instance:
   ```bash
   sudo -u postgres psql
   CREATE DATABASE trading_data;
   CREATE USER deployer WITH PASSWORD 'your_password';
   GRANT ALL PRIVILEGES ON DATABASE trading_data TO deployer;
   ```

## Database Configuration

The application uses a Digital Ocean managed PostgreSQL database:

### Connection Details
- Host: vvv-trading-db-do-user-21110609-0.i.db.ondigitalocean.com
- Port: 25060
- Database: defaultdb
- Username: doadmin
- SSL Mode: require

### Security Notes
- Never commit the .env file containing the database password
- The database cluster is open to all incoming connections by default
- Consider using the "Secure this database cluster by restricting access" option in DO
- Download and keep the CA certificate in a secure location

### Connection String Format
```python
DATABASE_URL = "postgresql://doadmin:${DB_PASSWORD}@vvv-trading-db-do-user-21110609-0.i.db.ondigitalocean.com:25060/defaultdb?sslmode=require"
```

### Important Specifications
- Primary Node: 1 vCPU / 1GB RAM / 10 GiB SSD
- Connection Limit: 22 concurrent connections
- PostgreSQL Version: 17
- VPC Network: default-ams3 (10.110.0.0/20)
- Location: Amsterdam (AMS3)

### Monitoring
- Monitor connection count (limit: 22)
- Monitor storage usage (limit: 10 GiB)
- Set up alerts for 80% usage thresholds

## Monitoring

1. **Log Files**
   - Main collector log: `/var/log/darkpool_collector/darkpool_collector.log`
   - Cron job log: `/var/log/darkpool_collector/cron.log`

2. **System Status**
   ```bash
   # Check if the collector is running
   ps aux | grep collect_darkpool_trades.py
   
   # Check database connection
   psql -U deployer -d trading_data -c "SELECT COUNT(*) FROM trading.darkpool_trades;"
   ```

## Troubleshooting

1. **Cron Job Issues**
   - Check cron logs: `grep CRON /var/log/syslog`
   - Verify Python path: `which python3`

2. **Database Connection Issues**
   - Check PostgreSQL status: `sudo systemctl status postgresql`
   - Verify connection: `psql -U deployer -d trading_data`

3. **API Issues**
   - Check API key validity
   - Verify network connectivity

## Maintenance

1. **Regular Updates**
   ```bash
   sudo apt-get update
   sudo apt-get upgrade
   pip3 install --upgrade -r requirements.txt
   ```

2. **Log Rotation**
   Add to `/etc/logrotate.d/darkpool_collector`:
   ```
   /var/log/darkpool_collector/*.log {
       daily
       missingok
       rotate 7
       compress
       delaycompress
       notifempty
       create 0640 deployer deployer
   }
   ``` 