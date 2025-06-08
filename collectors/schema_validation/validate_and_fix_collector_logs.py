import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env.prod
load_dotenv(dotenv_path='.env.prod')

db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSLMODE', 'prefer')
}

REQUIRED_COLUMNS = [
    ('id', 'SERIAL', 'PRIMARY KEY'),
    ('timestamp', 'TIMESTAMPTZ', 'NOT NULL'),
    ('collector_name', 'VARCHAR(50)', 'NOT NULL'),
    ('level', 'VARCHAR(16)', 'NOT NULL'),
    ('message', 'TEXT', 'NOT NULL'),
    ('task_type', 'VARCHAR(50)', ''),
    ('details', 'JSONB', ''),
    ('is_heartbeat', 'BOOLEAN', ''),
    ('status', 'VARCHAR(50)', ''),
    ('items_processed', 'INTEGER', ''),
    ('api_credits_used', 'INTEGER', ''),
    ('duration_seconds', 'FLOAT', ''),
    ('error_details', 'JSONB', ''),
]

def print_schema(cur):
    print("Current schema for trading.collector_logs:")
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'trading' AND table_name = 'collector_logs'
        ORDER BY ordinal_position
    """)
    for row in cur.fetchall():
        print(f"  {row[0]:20} {row[1]:15} {'NULLABLE' if row[2]=='YES' else 'NOT NULL'}")

def ensure_column(cur, col, dtype, constraint):
    # Check if column exists
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'trading' AND table_name = 'collector_logs' AND column_name = %s
    """, (col,))
    if not cur.fetchone():
        print(f"Adding missing column: {col} {dtype}")
        cur.execute(f"ALTER TABLE trading.collector_logs ADD COLUMN {col} {dtype};")
    # Set NOT NULL if required
    if 'NOT NULL' in constraint:
        cur.execute(f"""
            SELECT is_nullable FROM information_schema.columns
            WHERE table_schema = 'trading' AND table_name = 'collector_logs' AND column_name = %s
        """, (col,))
        nullable = cur.fetchone()[0]
        if nullable == 'YES':
            print(f"Setting {col} to NOT NULL")
            cur.execute(f"ALTER TABLE trading.collector_logs ALTER COLUMN {col} SET NOT NULL;")

def main():
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            print_schema(cur)
            # Ensure all required columns exist and have correct constraints
            for col, dtype, constraint in REQUIRED_COLUMNS:
                ensure_column(cur, col, dtype, constraint)
            conn.commit()
            print("\nSchema validation and correction complete.")
            print_schema(cur)

if __name__ == "__main__":
    main() 