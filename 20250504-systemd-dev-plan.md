# Systemd Migration Development Plan

## Overview
This document outlines the plan to migrate the Dark Pool Trade Collector from crontab-based execution to a systemd service. This change will provide better process management, automatic restarts, and improved monitoring capabilities.

## Current State
- Collector runs via crontab
- Logs are written to `/var/log/darkpool_collector/darkpool_collector.log`
- No automatic restart on failure
- Limited process management capabilities

## Migration Steps

### 1. Preparation (Day 1)
- [ ] Review current crontab configuration
- [ ] Document current process dependencies
- [ ] Create backup of current setup
- [ ] Test systemd service configuration in development environment

### 2. Service Configuration (Day 1-2)
- [ ] Create systemd service file
  ```ini
  [Unit]
  Description=Dark Pool Trade Collector
  After=network.target postgresql.service

  [Service]
  User=deployer
  Group=deployer
  WorkingDirectory=/opt/darkpool_collector
  Environment="PATH=/opt/darkpool_collector/venv/bin"
  Environment="DB_HOST=vvv-trading-db-do-user-21110609-0.i.db.ondigitalocean.com"
  Environment="DB_PORT=25060"
  Environment="DB_NAME=defaultdb"
  Environment="DB_USER=doadmin"
  ExecStart=/opt/darkpool_collector/venv/bin/python -m flow_analysis.scripts.darkpool_collector
  Restart=always
  RestartSec=10

  [Install]
  WantedBy=multi-user.target
  ```

- [ ] Configure logging
  - [ ] Set up log rotation
  - [ ] Configure log levels
  - [ ] Test log file permissions

### 3. Testing (Day 2-3)
- [ ] Test service in development environment
  - [ ] Verify startup
  - [ ] Test automatic restarts
  - [ ] Check log rotation
  - [ ] Verify database connections
  - [ ] Test error handling

- [ ] Create test scenarios
  - [ ] Service crash recovery
  - [ ] Database connection loss
  - [ ] Network issues
  - [ ] Resource constraints

### 4. Deployment (Day 3)
- [ ] Schedule maintenance window
- [ ] Backup current setup
- [ ] Deploy systemd service
- [ ] Remove crontab entry
- [ ] Verify service status
- [ ] Monitor for issues

### 5. Monitoring Setup (Day 3-4)
- [ ] Configure systemd journal logging
- [ ] Set up alerts for service failures
- [ ] Create monitoring dashboard
- [ ] Document monitoring procedures

## Rollback Plan
If issues arise during migration:
1. Stop systemd service
2. Restore crontab entry
3. Verify collector is running
4. Document issues for future reference

## Success Criteria
- [ ] Service starts automatically on boot
- [ ] Automatic restart on failure
- [ ] Proper log rotation
- [ ] No data loss during migration
- [ ] All monitoring in place
- [ ] Documentation updated

## Timeline
- Day 1: Preparation and configuration
- Day 2: Testing in development
- Day 3: Deployment to production
- Day 4: Monitoring setup and verification

## Resources Needed
1. Development environment
2. Test database instance
3. Monitoring tools
4. Documentation tools

## Risks and Mitigation
1. **Service Downtime**
   - Mitigation: Schedule during low-activity period
   - Rollback plan in place

2. **Data Loss**
   - Mitigation: Backup current setup
   - Verify data integrity before and after

3. **Configuration Issues**
   - Mitigation: Test in development first
   - Document all configuration changes

4. **Permission Problems**
   - Mitigation: Verify all file permissions
   - Test with correct user context

## Post-Migration Tasks
1. Update documentation
2. Train team on new monitoring
3. Schedule regular service health checks
4. Review and optimize configuration

## Future Improvements
1. Implement health check endpoint
2. Add metrics collection
3. Set up automated testing
4. Create deployment pipeline

## Sign-off
- [ ] Development Lead
- [ ] Operations Team
- [ ] Security Team
- [ ] Business Owner 