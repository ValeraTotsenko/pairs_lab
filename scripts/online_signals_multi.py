#!/usr/bin/env python3
import os, json, asyncio, ccxt.async_support as ccxt, numpy as np
from aiogram import Bot
import pandas as pd
import duckdb
from datetime import datetime

# Конфиг Telegram и Binance из переменных окружения
BINANCE_KEY = os.getenv("BINANCE_KEY")
BINANCE_SECRET = os.getenv("BINANCE_SECRET")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID   = int(os.getenv("TG_CHAT_ID"))

# Загружаем массив параметров
with open("params_multi.json") as f:
    PAIRS = json.load(f)

bot = Bot(TG_BOT_TOKEN)
exc = ccxt.binance({'apiKey': BINANCE_KEY, 'secret': BINANCE_SECRET, 'enableRateLimit': True})

# Быстрая подгрузка исторических данных из DuckDB (ускоряет работу)
def get_last_prices(pair, window):
    db = duckdb.connect("data/quotes.duckdb")
    df_a = db.sql(f"SELECT ts, close FROM klines WHERE symbol='{pair['pair_a']}' AND interval='1d' ORDER BY ts DESC LIMIT {window+5}").df()
    df_b = db.sql(f"SELECT ts, close FROM klines WHERE symbol='{pair['pair_b']}' AND interval='1d' ORDER BY ts DESC LIMIT {window+5}").df()
    db.close()
    df_a = df_a.drop_duplicates(subset='ts', keep='last').set_index('ts')
    df_b = df_b.drop_duplicates(subset='ts', keep='last').set_index('ts')
    df = pd.concat([df_a, df_b], axis=1, keys=['a', 'b']).dropna().iloc[-window:]
    return df['a'].values, df['b'].values

async def fetch_price_ccxt(symbol, window):
    # В случае чего можем заменить получение из API (тут — исторические данные)
    # ohlc = await exc.fetch_ohlcv(symbol, timeframe='1d', limit=window)
    # return np.array([c[4] for c in ohlc])
    return None

async def main():
    while True:
        for pair in PAIRS:
            try:
                # Загружаем цены (можно переключить на fetch_price_ccxt для реального онлайн-обновления)
                pA, pB = get_last_prices(pair, pair['window'])
                spread = np.log(pA[-1]) - np.log(pB[-1])
                hist_spread = np.log(pA) - np.log(pB)
                z = (spread - hist_spread.mean()) / hist_spread.std()
                now = datetime.now().strftime('%Y-%m-%d %H:%M')

                if z > pair['zin']:
                    txt = (f"🔴 {now} {pair['pair_a']} vs {pair['pair_b']}\n"
                           f"Z={z:.2f}: Short {pair['pair_a']}, Long {pair['pair_b']}, lev={pair['lev']}")
                    await bot.send_message(TG_CHAT_ID, txt)
                elif z < -pair['zin']:
                    txt = (f"🟢 {now} {pair['pair_a']} vs {pair['pair_b']}\n"
                           f"Z={z:.2f}: Long {pair['pair_a']}, Short {pair['pair_b']}, lev={pair['lev']}")
                    await bot.send_message(TG_CHAT_ID, txt)
            except Exception as e:
                print(f"Error for {pair['pair_a']}/{pair['pair_b']}: {e}")
        await asyncio.sleep(1800)  # Проверять каждые 30 минут

if __name__ == "__main__":
    asyncio.run(main())
