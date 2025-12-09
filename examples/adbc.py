# /// script
# requires-python = ">=3.9"
# dependencies = ["adbc-driver-manager>=1.8.0", "pyarrow>=20.0.0"]
# ///

### before running, run this on the command line:

# export motherduck_token="..."
# pipx install dbc
# dbc install duckdb
# pip install pyarrow adbc-driver-manager

import pyarrow as pa
from adbc_driver_manager import dbapi

con = dbapi.connect(driver="duckdb", db_kwargs={"path": "md:my_db"})

table = pa.table(
    [
        ["Tenacious D(uck)", "Backstreet Buoys", "Wu-Quack Clan"],
        [4, 10, 7]
    ],
    names=["name", "albums"],
)

with con.cursor() as cursor:
    cursor.adbc_ingest("groups", table)

with con.cursor() as cursor:
    cursor.execute("SELECT * FROM groups")
    table = cursor.fetch_arrow_table()

print(table)
