#!/bin/bash

# Create collector role if it doesn't exist
psql -d trading_data -c "DO \$\$ BEGIN CREATE ROLE collector WITH LOGIN PASSWORD 'your_db_password_here'; EXCEPTION WHEN duplicate_object THEN null; END \$\$;"

# Create schema and tables if they don't exist
psql -d trading_data -f schema.sql

# Grant permissions to collector role
psql -d trading_data -c "GRANT CONNECT ON DATABASE trading_data TO collector;"
psql -d trading_data -c "GRANT USAGE ON SCHEMA trading TO collector;"
psql -d trading_data -c "GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA trading TO collector;"
psql -d trading_data -c "GRANT USAGE ON ALL SEQUENCES IN SCHEMA trading TO collector;" 