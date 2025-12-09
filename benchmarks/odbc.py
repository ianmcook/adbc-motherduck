# /// script
# requires-python = ">=3.9"
# dependencies = ["pyodbc"]
# ///

### before running:

# run this on the command line:
# export motherduck_token="..."

# follow the setup instructions for your platform at https://duckdb.org/docs/stable/clients/odbc/overview

import os
import time
import pyodbc

# Configuration
NUM_RUNS = 5 # Number of times to run the benchmark
SCALE_FACTOR = 0.01  # TPC-H scale factor (see https://duckdb.org/docs/stable/core_extensions/tpch)

# Get MotherDuck token from environment variable
motherduck_token = os.getenv("motherduck_token")

# ODBC connection string for MotherDuck
connection_string = (
    "Driver={DuckDB Driver};"
    f"motherduck_token={motherduck_token};"
    "database=md:my_db;"
)

# Connect to MotherDuck
con = pyodbc.connect(connection_string, autocommit=True)

# Generate TPC-H data
with con.cursor() as cursor:
    cursor.execute("DROP TABLE IF EXISTS lineitem")
    cursor.execute(f"CALL DBGEN(sf={SCALE_FACTOR})")

# Benchmark query execution and result fetching
query_times = []

for i in range(NUM_RUNS):
    start_time = time.time()
    
    # Execute query and fetch result
    with con.cursor() as cursor:
        cursor.execute("SELECT * FROM lineitem")
        result = cursor.fetchall()
    
    end_time = time.time()
    duration_ms = (end_time - start_time) * 1000  # Convert to milliseconds
    query_times.append(duration_ms)
    
    print(f"Run {i+1}/{NUM_RUNS}: {duration_ms:.2f} ms")

# Sort times to find median
query_times.sort()

# Calculate median (for 5 runs, median is at index 2)
median_index = len(query_times) // 2
median_time = query_times[median_index]

print(f"\n[ODBC] Median time: {median_time:.2f} ms")

# Close connection
con.close()
