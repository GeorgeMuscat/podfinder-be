import os
import psycopg

PG_HOST = os.getenv("PGHOST", "localhost")
PG_USER = os.getenv("PGUSER", "postgres")
PG_PASSWORD = os.getenv("PGPASSWORD", "devpassword")
PG_DATABASE = os.getenv("PGDATABASE", "stats")
PG_PORT = os.getenv("PGPORT", "5432")

DB_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

conn = psycopg.connect(DB_URL)
