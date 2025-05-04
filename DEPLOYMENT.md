# Dark Pool Trade Collector Deployment Guide

## Prerequisites
- Digital Ocean droplet (Ubuntu 20.04 or later recommended)
- Digital Ocean managed PostgreSQL database
- SSH access to the droplet
- DO API token for database access

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

4. **Clone the repository**
   ```bash
   su - deployer
   git clone <repository-url>
   cd darkpool-collector
   ```

5. **Run the deployment script**
   ```bash
   chmod +x deploy-do.sh
   ./deploy-do.sh
   ```

6. **Verify the installation**
   ```bash
   # Check service status
   sudo systemctl status darkpool_collector
   
   # Check logs
   tail -f /var/log/darkpool_collector/darkpool_collector.log
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

## Service Management

### Start/Stop/Restart the service
```bash
sudo systemctl start darkpool_collector
sudo systemctl stop darkpool_collector
sudo systemctl restart darkpool_collector
```

### Check service status
```bash
sudo systemctl status darkpool_collector
```

### View logs
```bash
# View current logs
tail -f /var/log/darkpool_collector/darkpool_collector.log

# View archived logs
ls -l /var/log/darkpool_collector/
```

## Configuration

The application configuration is located in:
- `/etc/darkpool_collector/` - Configuration files
- `/opt/darkpool_collector/` - Application files
- `/var/log/darkpool_collector/` - Log files

## Log Rotation

Logs are automatically rotated daily and kept for 14 days. The configuration is in:
```bash
/etc/logrotate.d/darkpool_collector
```

## Troubleshooting

1. **Service won't start**
   - Check systemd logs: `journalctl -u darkpool_collector`
   - Verify database connection
   - Check file permissions

2. **Logs not being written**
   - Verify log directory permissions
   - Check disk space
   - Verify logrotate configuration

3. **Database connection issues**
   - Verify PostgreSQL is running
   - Check connection string in configuration
   - Verify database user permissions
   - Check if IP is whitelisted in DO
   - Verify SSL certificate configuration

## Security Considerations

1. **File Permissions**
   - Application files: 755
   - Configuration files: 640
   - Log files: 640

2. **Service User**
   - Runs as non-root user
   - Limited system access
   - Specific database permissions

3. **Network Security**
   - Only necessary ports open
   - Database access restricted to specific IPs
   - API keys properly secured
   - SSL required for database connections

## Backup and Recovery

1. **Database Backup**
   ```bash
   # Create backup
   pg_dump -U doadmin -h vvv-trading-db-do-user-21110609-0.i.db.ondigitalocean.com -p 25060 -d defaultdb --ssl-mode=require > backup.sql
   
   # Restore from backup
   psql -U doadmin -h vvv-trading-db-do-user-21110609-0.i.db.ondigitalocean.com -p 25060 -d defaultdb --ssl-mode=require < backup.sql
   ```

2. **Configuration Backup**
   ```bash
   # Backup configuration
   tar -czf config_backup.tar.gz /etc/darkpool_collector/
   
   # Restore configuration
   tar -xzf config_backup.tar.gz -C /
   ```

## Monitoring

1. **Service Health**
   - Systemd status
   - Log file monitoring
   - Database connection checks

2. **Performance Metrics**
   - CPU usage
   - Memory usage
   - Disk space
   - Database performance
   - Connection count monitoring

## Updates

To update the application:

1. Pull the latest changes
2. Run the deployment script
3. Restart the service

```bash
cd /opt/darkpool_collector
git pull
./deploy-do.sh
sudo systemctl restart darkpool_collector
``` 