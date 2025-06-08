import ccxt, duckdb, datetime as dt
exc = ccxt.binance()
symbols = [s for s in exc.load_markets() if s.endswith('/USDT')]
since = exc.parse8601('2023-01-01T00:00:00Z')
db = duckdb.connect('data/quotes.duckdb')
for sym in symbols:
    print(sym)
    ohlc = exc.fetch_ohlcv(sym, timeframe='1d', since=since, limit=1000)
    rows = [( 'binance', sym, '1d',
              dt.datetime.utcfromtimestamp(c[0]/1000), *c[1:]) for c in ohlc]
    db.execute("insert into klines values ?", rows)
