# we want to graph change in section1513alloc adjusted for inflation with the change in rvh 4 years later

import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
import numpy as np
import plotly.express as px

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

# adjust section1513 allocation for inflation
# Using Consumer Price Index (CPI) inflation rates - base year 2023
inflation_factors = {
    2006: 1.46,  # 2006 to 2023 inflation factor
    2007: 1.42,
    2008: 1.38,
    2009: 1.38,
    2010: 1.36,
    2011: 1.32,
    2012: 1.30,
    2013: 1.28,
    2014: 1.27,
    2015: 1.27,
    2016: 1.25,
    2017: 1.22,
    2018: 1.19,
    2019: 1.17,
    2020: 1.16,
    2021: 1.10,
    2022: 1.02,
    2023: 1.00   # base year
}

# Fetch sec1513alloc data from database
cur.execute("SELECT department, year, value FROM sec1513alloc ORDER BY department, year")
sec1513_data = cur.fetchall()

# Convert to DataFrame
df_sec1513 = pd.DataFrame(sec1513_data, columns=['department', 'year', 'value'])

# Apply inflation adjustment
df_sec1513['inflation_factor'] = df_sec1513['year'].map(inflation_factors)
df_sec1513['adjusted_value'] = df_sec1513['value'] * df_sec1513['inflation_factor']

print("Section 1513 allocation data adjusted for inflation:")
print(df_sec1513.head(10))

# Create table for inflation-adjusted data
cur.execute("""
    CREATE TABLE IF NOT EXISTS sec1513alloc_adjusted (
        id SERIAL PRIMARY KEY,
        department TEXT,
        year INT,
        original_value INT,
        inflation_factor DECIMAL(4,2),
        adjusted_value DECIMAL(12,2)
    )
""")

# Clear existing data
cur.execute("DELETE FROM sec1513alloc_adjusted")

# Insert inflation-adjusted data
for _, row in df_sec1513.iterrows():
    cur.execute("""
        INSERT INTO sec1513alloc_adjusted (department, year, original_value, inflation_factor, adjusted_value)
        VALUES (%s, %s, %s, %s, %s)
    """, (row['department'], row['year'], row['value'], row['inflation_factor'], row['adjusted_value']))

conn.commit()
print("Inflation-adjusted Section 1513 allocation data saved to database")

# grab rvh data
cur.execute("SELECT department, year, value FROM rvh ORDER BY department, year")
rvh_data = cur.fetchall()

df_rvh = pd.DataFrame(rvh_data, columns=['department', 'year', 'value'])

# grab rvm data
cur.execute("SELECT department, year, value FROM rvm ORDER BY department, year")
rvm_data = cur.fetchall()

df_rvm = pd.DataFrame(rvm_data, columns=['department', 'year', 'value'])

# grab total passenger data
cur.execute("SELECT department, year, value FROM totalpassengers ORDER BY department, year")
total_passengers_data = cur.fetchall()

df_total_passengers = pd.DataFrame(total_passengers_data, columns=['department', 'year', 'value'])

# Close connection
cur.close()
conn.close()

# 1,000,000 total passengers

# Note: You can use Plotly or Matplotlib to visualize the data as needed.
# Example: Plot percent change in adjusted section1513alloc vs percent change in rvh 4 years later

# Calculate percent change in adjusted section1513alloc by department and year
df_sec1513['pct_change_adj'] = df_sec1513.groupby('department')['adjusted_value'].pct_change() * 100

# Calculate percent change in rvh by department and year
df_rvh['pct_change_rvh'] = df_rvh.groupby('department')['value'].pct_change() * 100

# Create year+4 for section1513 to match with future RVH data
df_sec1513['year_plus_4'] = df_sec1513['year'] + 2

# Merge: section1513 change in year X with RVH change in year X+4
merged = pd.merge(
    df_sec1513[['department', 'year', 'year_plus_4', 'pct_change_adj']],
    df_rvh[['department', 'year', 'pct_change_rvh']],
    left_on=['department', 'year_plus_4'],
    right_on=['department', 'year'],
    how='inner',
    suffixes=('_sec1513', '_rvh')
)

#filter out rows where the department has less than 1,000,000 total passengers in that year
merged = pd.merge(
    merged,
    df_total_passengers[['department', 'year', 'value']],
    left_on=['department', 'year_sec1513'],
    right_on=['department', 'year'],
    how='inner'
)
merged = merged[merged['value'] >= 1000000]

# Add meaningful column names and show the relationship
merged['sec1513_year'] = merged['year_sec1513']
merged['rvh_year'] = merged['year_rvh']
print("Merged data showing Section1513 year vs RVH year (should be +4):")
print(merged[['department', 'sec1513_year', 'rvh_year', 'pct_change_adj', 'pct_change_rvh']].head(10))

# Drop rows with NaN percent changes
merged = merged.dropna(subset=['pct_change_adj', 'pct_change_rvh'])
merged = merged.replace([np.inf, -np.inf], np.nan).dropna(subset=['pct_change_adj', 'pct_change_rvh'])

print(f"Data points after cleaning: {len(merged)}")
print("Summary of percent changes:")
print(f"Section1513 change range: {merged['pct_change_adj'].min():.1f}% to {merged['pct_change_adj'].max():.1f}%")
print(f"RVH change range: {merged['pct_change_rvh'].min():.1f}% to {merged['pct_change_rvh'].max():.1f}%")

# Plot using Plotly
fig = px.scatter(
    merged,
    x='pct_change_adj',
    y='pct_change_rvh',
    color='department',
    labels={
        'pct_change_adj': 'Percent Change in Adjusted Section1513Alloc',
        'pct_change_rvh': 'Percent Change in RVH (4 years later)',
    },
    hover_data=['sec1513_year', 'rvh_year', 'value'], # 'value' is total_passengers
    title='Section1513Alloc (Inflation Adjusted) vs RVH Change (4 Years Later)',
    trendline="ols"
)
fig.show()