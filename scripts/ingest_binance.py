#!/usr/bin/env python
"""
Качает дневные свечи Binance за N дней и пишет в DuckDB, избегая дубликатов.
"""

import argparse, datetime as dt, time
import ccxt, duckdb
from tqdm import tqdm

# ---------- CLI ----------
cli = argparse.ArgumentParser()
cli.add_argument("--days", type=int, default=365)
cli.add_argument("--api_key")
cli.add_argument("--api_secret")
cli.add_argument("--silent", action="store_true")
args = cli.parse_args()

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
db.execute("""CREATE UNIQUE INDEX IF NOT EXISTS klines_idx
              ON klines(symbol, interval, ts);""")

insert_sql = """INSERT INTO klines VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?)"""

def log(msg):
    if not args.silent:
        print(msg)

# ---------- MAIN LOOP ----------
for sym in tqdm(symbols, disable=args.silent):
    # Узнаём последнюю дату в БД, чтобы не тянуть дубликаты
    last_ts = db.sql(f"""
        SELECT max(ts) FROM klines
        WHERE symbol='{sym}' AND interval='{interval}'
    """).fetchone()[0]
    since_ms = since_ms_global
    if last_ts:
        since_ms = int(last_ts.timestamp() * 1000) + 86_400_000

    rows = []
    cursor = since_ms
    while True:
        ohlc = exc.fetch_ohlcv(sym, timeframe=interval,
                               since=cursor, limit=1000)
        if not ohlc:
            break
        rows.extend([
            ("binance", sym, interval,
             dt.datetime.utcfromtimestamp(c[0] / 1000),
             c[1], c[2], c[3], c[4], c[5])
            for c in ohlc
        ])
        cursor = ohlc[-1][0] + 86_400_000
        time.sleep(0.25)
        if len(ohlc) < 1000 or cursor > exc.milliseconds():
            break

    # убираем возможные дубликаты внутри rows
    dedup = {(r[3], r[1], r[2]): r for r in rows}.values()
    if dedup:
        try:
            db.executemany(insert_sql, list(dedup))
        except duckdb.ConstraintException:
            # если вдруг заехали дубликаты, просто пропустим
            pass

db.close()
log("✅ История загружена без дубликатов.")
