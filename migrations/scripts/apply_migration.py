import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env or .env.prod
env_file = '.env.prod' if os.getenv('ENV') == 'prod' else '.env'
load_dotenv(env_file)

# Read database connection details from environment variables
db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Path to the migration file
migration_file = 'migrations/remove_url_column.sql'

# Read the migration SQL
with open(migration_file, 'r') as f:
    migration_sql = f.read()

# Connect to the database and execute the migration
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute(migration_sql)
    conn.commit()
    print("Migration applied successfully.")
except Exception as e:
    print(f"Error applying migration: {e}")
finally:
    if conn:
        conn.close() 