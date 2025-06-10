#!/usr/bin/env python
"""
Загрузка дневных свечей Binance в DuckDB без дублирующихся (symbol,interval,ts)

Пример:
  python scripts/ingest_binance.py --days 365 --api_key $BINANCE_KEY \
                                   --api_secret $BINANCE_SECRET --silent
"""
import argparse, datetime as dt, time
import duckdb, ccxt
from tqdm import tqdm

# ---------- CLI ----------
cli = argparse.ArgumentParser()
cli.add_argument("--days", type=int, default=365)
cli.add_argument("--api_key")
cli.add_argument("--api_secret")
cli.add_argument("--silent", action="store_true")
args = cli.parse_args()

def log(msg):
    if not args.silent:
        print(msg)

# ---------- EXCHANGE ----------
exc = ccxt.binance({
    "apiKey": args.api_key, "secret": args.api_secret,
    "enableRateLimit": True,
})
symbols = [s for s in exc.load_markets() if s.endswith("/USDT")]
interval = "1d"
since_ms_global = exc.milliseconds() - args.days * 86_400_000  # ms

# ---------- DB ----------
db = duckdb.connect("data/quotes.duckdb")
db.execute("CREATE TABLE IF NOT EXISTS klines ("
           " exchange VARCHAR, symbol VARCHAR, interval VARCHAR,"
           " ts TIMESTAMP, open DOUBLE, high DOUBLE, low DOUBLE,"
           " close DOUBLE, volume DOUBLE);")

# 1) Очистка дублей (разово)
dup_rows = db.sql("""
    SELECT COUNT(*) FROM (
      SELECT symbol, interval, ts, COUNT(*) AS c
      FROM klines GROUP BY 1,2,3 HAVING c > 1
    )
""").fetchone()[0]
if dup_rows:
    log(f"⚠️  Найдено дубликатов: {dup_rows} — очищаю…")
    db.execute("""
        CREATE TABLE klines_clean AS
        SELECT DISTINCT * FROM klines;
    """)
    db.execute("DROP TABLE klines;")
    db.execute("ALTER TABLE klines_clean RENAME TO klines;")
    log("✅ Дубликаты удалены.")

# 2) Индекс (создаём, если ещё нет)
try:
    db.execute("CREATE UNIQUE INDEX klines_idx "
               "ON klines(symbol, interval, ts);")
except duckdb.ConstraintException:
    # значит дубликаты всё ещё остались — но их уже нет,
    # или индекс создан ранее; пропускаем
    pass

insert_sql = """INSERT INTO klines VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?)"""

# ---------- Загрузка ----------
for sym in tqdm(symbols, disable=args.silent):
    # берём последний ts для символа
    last_ts = db.sql(f"""
        SELECT max(ts) FROM klines
        WHERE symbol='{sym}' AND interval='{interval}'
    """).fetchone()[0]
    since_ms = since_ms_global if last_ts is None \
        else int(last_ts.timestamp() * 1000) + 86_400_000

    cursor = since_ms
    while True:
        ohlc = exc.fetch_ohlcv(sym, timeframe=interval,
                               since=cursor, limit=1000)
        if not ohlc:
            break

        rows = [
            ("binance", sym, interval,
             dt.datetime.utcfromtimestamp(c[0] / 1000),
             c[1], c[2], c[3], c[4], c[5])
            for c in ohlc
        ]

        # executemany пропустит записи, нарушающие UNIQUE индекс
        db.executemany(insert_sql, rows)

        cursor = ohlc[-1][0] + 86_400_000
        time.sleep(0.25)
        if len(ohlc) < 1000 or cursor > exc.milliseconds():
            break

db.close()
log("✅ История загружена и индексирована.")
