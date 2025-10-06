from dotenv import load_dotenv
import os
import psycopg2

import numpy as np
import pandas as pd

# ---- READ .env ----
load_dotenv()

db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)

print("Connecting to:", db_name, "as", db_user)
cur = conn.cursor()

# ---- READ Section 1513 data ----
cur.execute("SELECT department, year, value FROM sec1513alloc ORDER BY department, year")
sec1513_data = cur.fetchall()

# ---- PROCESS Section 1513 data ----
df_sec1513 = pd.DataFrame(sec1513_data, columns=['department', 'year', 'value'])

# adjust section1513 allocation for inflation
# Using Consumer Price Index (CPI) inflation rates - base year 2007
inflation_factors = {
    2007: 207.342,
    2008: 215.303,
    2009: 214.537,
    2010: 218.056,
    2011: 224.939,
    2012: 229.594,
    2013: 232.957,
    2014: 236.736,
    2015: 237.017,
    2016: 240.007,
    2017: 245.120,
    2018: 251.107,
    2019: 255.657,
    2020: 258.811,
    2021: 270.970,
    2022: 292.655,
    2023: 304.702,
    2024: 313.689
}

df_sec1513['inflation_factor'] = df_sec1513['year'].map(inflation_factors)
df_sec1513['adjusted_value'] = df_sec1513['value'] * (df_sec1513['inflation_factor'] / inflation_factors[2007])
print(df_sec1513.head(10))

# ---- WRITE adjusted Section 1513 data back to database ----
cur.execute("""
    CREATE TABLE IF NOT EXISTS sec1513alloc_adjusted (
        id SERIAL PRIMARY KEY,
        department TEXT,
        year INT,
        value DECIMAL(12, 2)
    )
""")

cur.execute("DELETE FROM sec1513alloc_adjusted")

for _, row in df_sec1513.iterrows():
    cur.execute("""
        INSERT INTO sec1513alloc_adjusted (department, year, value)
        VALUES (%s, %s, %s)
    """, (row['department'], row['year'], row['adjusted_value']))
conn.commit()
print("Inflation-adjusted Section 1513 allocation data inserted into sec1513alloc_adjusted table.")