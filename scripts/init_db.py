#!/usr/bin/env python
import os, duckdb

os.makedirs("data", exist_ok=True)
con = duckdb.connect("data/quotes.duckdb")
con.execute("""
CREATE TABLE IF NOT EXISTS klines (
  exchange   VARCHAR,
  symbol     VARCHAR,
  interval   VARCHAR,
  ts         TIMESTAMP,
  open       DOUBLE,
  high       DOUBLE,
  low        DOUBLE,
  close      DOUBLE,
  volume     DOUBLE
);
""")
con.close()
print("✅ DuckDB и таблица klines готовы.")
