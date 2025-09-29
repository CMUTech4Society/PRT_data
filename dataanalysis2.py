from dotenv import load_dotenv
import os

import numpy as np
import pandas as pd
import plotly.express as px
import psycopg2

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

df_sec1513 = pd.DataFrame(sec1513_data, columns=['department', 'year', 'value'])
print(df_sec1513.head(10))

# ---- READ RVH data ----
cur.execute("SELECT department, year, value FROM rvh ORDER BY department, year")
rvh_data = cur.fetchall()

df_rvh = pd.DataFrame(rvh_data, columns=['department', 'year', 'value'])
print(df_rvh.head(10))

# ---- READ RVM data ----
cur.execute("SELECT department, year, value FROM rvm ORDER BY department, year")
rvm_data = cur.fetchall()

df_rvm = pd.DataFrame(rvm_data, columns=['department', 'year', 'value'])
print(df_rvm.head(10))

# ---- READ Total Passenger data ----
cur.execute("SELECT department, year, value FROM totalpassengers ORDER BY department, year")
total_passengers_data = cur.fetchall()

df_total_passengers = pd.DataFrame(total_passengers_data, columns=['department', 'year', 'value'])
print(df_total_passengers.head(10))

# ---- Close connection ----
cur.close()
conn.close()

# ---------------- Data Analysis ----------------
# Merge Section 1513 with Total Passengers on department and year
df_merged = pd.merge(df_sec1513, df_total_passengers, on=['department', 'year'], suffixes=('_sec1513', '_totalpassengers'))
print("Merged DataFrame head:")

# Clean data: remove rows with zero or missing values that could cause division issues
df_sec1513_clean = df_merged[(df_merged['value_sec1513'] > 0) & (df_merged['value_totalpassengers'] > 0)].copy()

print(f"Original data points: {len(df_sec1513)}")
print(f"Clean data points: {len(df_sec1513_clean)}")

print(df_sec1513_clean.head(10))

# exclude SEPTA and PRT by excluding things more than 50 million passengers or 200 million in allocation
df_sec1513_clean = df_sec1513_clean[(df_sec1513_clean['value_totalpassengers'] < 50000000) & (df_sec1513_clean['value_sec1513'] < 200000000)]

# Plot Section 1513 allocation compared to Total Passengers
fig = px.scatter(
    df_sec1513_clean,
    x='value_totalpassengers',
    y='value_sec1513',
    color='department',
    title='Section 1513 Allocation Over Years by Department',
    labels={'value_sec1513': 'Allocation Value', 'value_totalpassengers': 'Total Passengers'}
)
fig.show()