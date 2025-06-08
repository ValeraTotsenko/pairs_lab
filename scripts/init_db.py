# init_db.py
import os
import duckdb

# 1. Папка для базы
os.makedirs("data", exist_ok=True)

# 2. Подключаемся / создаём базу
con = duckdb.connect("data/quotes.duckdb")

# 3. Таблица свечей
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
