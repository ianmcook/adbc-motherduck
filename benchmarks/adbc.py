# /// script
# requires-python = ">=3.9"
# dependencies = ["adbc-driver-manager>=1.8.0", "pyarrow>=20.0.0"]
# ///

### before running, run this on the command line:

# export motherduck_token="..."
# pipx install dbc
# dbc install duckdb
# pip install pyarrow adbc-driver-manager

import time
from adbc_driver_manager import dbapi

# Configuration
NUM_RUNS = 5 # Number of times to run the benchmark
SCALE_FACTOR = 0.01  # TPC-H scale factor (see https://duckdb.org/docs/stable/core_extensions/tpch)

# Connect to MotherDuck
con = dbapi.connect(driver="duckdb", db_kwargs={"path": "md:my_db"})

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
        result = cursor.fetch_arrow_table()
    
    end_time = time.time()
    duration_ms = (end_time - start_time) * 1000  # Convert to milliseconds
    query_times.append(duration_ms)
    
    print(f"Run {i+1}/{NUM_RUNS}: {duration_ms:.2f} ms")

# Sort times to find median
query_times.sort()

# Calculate median (for 5 runs, median is at index 2)
median_index = len(query_times) // 2
median_time = query_times[median_index]

print(f"\n[ADBC] Median time: {median_time:.2f} ms")

# Close connection
con.close()
