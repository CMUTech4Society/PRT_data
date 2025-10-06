import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
import glob
import re

# --- READ .env ---
load_dotenv()

db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

# ---- Create database if not exists (recreate if it does) ----
conn = psycopg2.connect(
    dbname="postgres",
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)

conn.autocommit = True
cur = conn.cursor()

cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'")
exists = cur.fetchone()
if exists:
    cur.execute(f"DROP DATABASE {db_name}")
    print(f"Database {db_name} dropped.")
cur.execute(f"CREATE DATABASE {db_name}")
print(f"Database {db_name} created.")

cur.close()
conn.close()

# ---- Connect to PostgreSQL ----
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
print("Connecting to:", db_name, "as", db_user)
cur = conn.cursor()

csv_files = glob.glob("*.csv")
for csv_path in csv_files:
    filename = os.path.splitext(os.path.basename(csv_path))[0]

    # ---- Read CSV ----
    df = pd.read_csv(csv_path)
    df.rename({df.columns[0]: 'Department'}, inplace=True)

    # ---- Clean Data ----
    df = df.fillna("0")
    for i in range(2006, 2024):
        df[str(i)] = df[str(i)].map(lambda x: str(x).replace('$', '').replace(',', '')).map(lambda x: int(x) if x.replace('-', '').isdigit() else 0)

    # Convert from wide → long format
    df_long = df.melt(id_vars=["Department"],
                      var_name="year",
                      value_name="value")

    # ---- Create table ----
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {filename} (
            id SERIAL PRIMARY KEY,
            department TEXT,
            year INT,
            value INT
        )
    """)

    # ---- Insert rows ----
    for _, row in df_long.iterrows():
        cur.execute(f"""
            INSERT INTO {filename} (department, year, value)
            VALUES (%s, %s, %s)
        """, (row["Department"], int(row["year"]), row["value"]))
    print("Added", filename, "to database")

# ---- Commit & Close connection ----
conn.commit()
cur.close()
conn.close()
