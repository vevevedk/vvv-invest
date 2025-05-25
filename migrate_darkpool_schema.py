import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv('.env')

# Database connection parameters
db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSLMODE', 'disable')
}

# Connect to the database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

try:
    # Alter column types
    cur.execute("ALTER TABLE trading.darkpool_trades ALTER COLUMN tracking_id TYPE character varying(50) USING tracking_id::text;")
    cur.execute("ALTER TABLE trading.darkpool_trades ALTER COLUMN symbol TYPE character varying(32) USING symbol::character varying(32);")
    cur.execute("ALTER TABLE trading.darkpool_trades ALTER COLUMN volume TYPE integer USING volume::integer;")
    cur.execute("ALTER TABLE trading.darkpool_trades ALTER COLUMN executed_at TYPE timestamp without time zone USING executed_at AT TIME ZONE 'UTC';")

    # Add missing columns if they do not exist
    missing_columns = [
        ('ext_hour_sold_codes', 'character varying(50)'),
        ('trade_code', 'character varying(50)'),
        ('trade_settlement', 'character varying(50)'),
        ('canceled', 'boolean DEFAULT false'),
        ('id', 'serial UNIQUE')
    ]
    for col, coltype in missing_columns:
        cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'trading' AND table_name = 'darkpool_trades' AND column_name = '{col}') THEN
                    ALTER TABLE trading.darkpool_trades ADD COLUMN {col} {coltype};
                END IF;
            END$$;
        """)

    # Set NOT NULL constraints
    not_null_columns = ['tracking_id', 'symbol', 'size', 'price', 'volume', 'premium', 'executed_at', 'collection_time']
    for col in not_null_columns:
        cur.execute(f"ALTER TABLE trading.darkpool_trades ALTER COLUMN {col} SET NOT NULL;")

    # Alter additional column types
    cur.execute("ALTER TABLE trading.darkpool_trades ALTER COLUMN sale_cond_codes TYPE character varying(50) USING sale_cond_codes::character varying(50), ALTER COLUMN market_center TYPE character varying(32) USING market_center::character varying(32);")

    # Commit the changes
    conn.commit()
    print('Local schema updated to match production.')

except Exception as e:
    conn.rollback()
    print(f'Error: {e}')
finally:
    cur.close()
    conn.close() 