import os
import psycopg2
from dotenv import load_dotenv

import sys

# Usage: python apply_darkpool_migration.py [--prod]
use_prod = '--prod' in sys.argv

env_file = '.env.prod' if use_prod else '.env'
load_dotenv(env_file)

db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSLMODE', 'prefer')
}

migration_file = 'migrations/add_darkpool_columns.sql'

with open(migration_file, 'r') as f:
    migration_sql = f.read()

try:
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute(migration_sql)
    conn.commit()
    print(f"Migration applied successfully to {'production' if use_prod else 'local'} database.")
except Exception as e:
    print(f"Error applying migration: {e}")
finally:
    if 'conn' in locals():
        conn.close() 