#!/usr/bin/env python
import os, json, asyncio, ccxt.async_support as ccxt, numpy as np
from aiogram import Bot

CFG = json.load(open("params.json"))
A, B = CFG["pair_a"], CFG["pair_b"]

BINANCE_KEY = os.getenv("BINANCE_KEY")
BINANCE_SECRET = os.getenv("BINANCE_SECRET")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID   = int(os.getenv("TG_CHAT_ID"))

bot = Bot(TG_BOT_TOKEN)
exc = ccxt.binance({'apiKey': BINANCE_KEY, 'secret': BINANCE_SECRET,
                    'enableRateLimit': True})

async def fetch_price(sym):
    ohlc = await exc.fetch_ohlcv(sym, timeframe='1h', limit=CFG["window"])
    return np.array([c[4] for c in ohlc])

async def main():
    while True:
        pA, pB = await asyncio.gather(fetch_price(A), fetch_price(B))
        spread = np.log(pA[-1]) - np.log(pB[-1])
        z = (spread - np.log(pA/pB).mean()) / np.log(pA/pB).std()
        if z > CFG["zin"]:
            txt = f"🔴 Z={z:.2f}: Short {A}, Long {B}, lev={CFG['lev']}"
            await bot.send_message(TG_CHAT_ID, txt)
        elif z < -CFG["zin"]:
            txt = f"🟢 Z={z:.2f}: Long {A}, Short {B}, lev={CFG['lev']}"
            await bot.send_message(TG_CHAT_ID, txt)
        await asyncio.sleep(1800)  # 30 минут

if __name__ == "__main__":
    asyncio.run(main())
