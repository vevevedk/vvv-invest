import os
import psycopg2
from dotenv import load_dotenv

# Set ENV_FILE for downstream imports
os.environ['ENV_FILE'] = os.getenv('ENV_FILE', '.env')

# Load environment variables from ENV_FILE or default to .env
env_file = os.getenv('ENV_FILE', '.env')
load_dotenv(env_file, override=True)

# Database connection parameters from environment variables
db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSLMODE', 'prefer')
}

# Connect to the database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Close the cursor and connection
cur.close()
conn.close()

# Reconnect to the database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Close the cursor and connection again
cur.close()
conn.close()

print("Database connection restarted successfully.") 