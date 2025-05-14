import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env.prod
load_dotenv(dotenv_path='.env.prod')

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

# SQL command to add the content column
sql_command = "ALTER TABLE trading.news_headlines ADD COLUMN content TEXT;"

# Execute the SQL command
cur.execute(sql_command)

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

print("Column 'content' added to trading.news_headlines table.") 