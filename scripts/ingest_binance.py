#!/usr/bin/env python
import argparse, datetime as dt, time
import ccxt, duckdb
from tqdm import tqdm

# ---------------- CLI ----------------
p = argparse.ArgumentParser()
p.add_argument("--days", type=int, default=365, help="Сколько дней истории качать")
p.add_argument("--api_key")
p.add_argument("--api_secret")
p.add_argument("--silent", action="store_true")
args = p.parse_args()

# --------------- EXCHANGE -----------
exc = ccxt.binance({
    "apiKey": args.api_key, "secret": args.api_secret,
    "enableRateLimit": True,
})
symbols = [s for s in exc.load_markets() if s.endswith("/USDT")]

since_ms = exc.milliseconds() - args.days * 86_400_000    # 1 day in ms
interval = "1d"

# ---------------- DB -----------------
db = duckdb.connect("data/quotes.duckdb")
insert_sql = """INSERT INTO klines VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?)"""

def log(msg):
    if not args.silent:
        print(msg)

# ------------- MAIN LOOP ------------
for sym in tqdm(symbols, disable=args.silent):
    rows, cursor_since = [], since_ms
    while True:
        ohlc = exc.fetch_ohlcv(sym, timeframe=interval,
                               since=cursor_since, limit=1000)
        if not ohlc:
            break
        rows.extend([
            ("binance", sym, interval,
             dt.datetime.utcfromtimestamp(c[0] / 1000),
             c[1], c[2], c[3], c[4], c[5]) for c in ohlc
        ])
        cursor_since = ohlc[-1][0] + 86_400_000
        time.sleep(0.2)
        if len(ohlc) < 1000 or cursor_since > exc.milliseconds():
            break
    if rows:
        db.executemany(insert_sql, rows)
db.close()
log("✅ История свечей загружена.")
