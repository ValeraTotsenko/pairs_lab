import json, asyncio, ccxt.async_support as ccxt, numpy as np
from aiogram import Bot

bot = Bot('BOT_TOKEN')
with open('params.json') as f: pars = json.load(f)
w, zin, zout, lev = pars

exc = ccxt.binance({'enableRateLimit': True})
symbols = ['UNI/USDT','SUSHI/USDT']   # заменить на реальную пару

async def main():
    while True:
        ohlc1 = await exc.fetch_ohlcv(symbols[0], '1h', limit=w)
        ohlc2 = await exc.fetch_ohlcv(symbols[1], '1h', limit=w)
        p1 = np.array([c[4] for c in ohlc1])
        p2 = np.array([c[4] for c in ohlc2])
        spread = np.log(p1[-1]) - np.log(p2[-1])
        m = np.log(p1) - np.log(p2)
        m = m[-w:].mean(); s = m[-w:].std()
        z = (spread - m)/s
        if z > zin:
            await bot.send_message(chat_id=123456,
                text=f"Z={z:.2f} > {zin}: Short {symbols[0]}, Long {symbols[1]} (lev {lev})")
        elif z < -zin:
            await bot.send_message(chat_id=123456,
                text=f"Z={z:.2f} < -{zin}: Long {symbols[0]}, Short {symbols[1]} (lev {lev})")
        await asyncio.sleep(1800)  # раз в 30 мин

asyncio.run(main())
