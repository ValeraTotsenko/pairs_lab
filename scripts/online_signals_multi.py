import os, json, asyncio, sqlite3, shelve
import ccxt.async_support as ccxt
import numpy as np
import pandas as pd
import duckdb
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor

BINANCE_KEY = os.getenv("BINANCE_KEY")
BINANCE_SECRET = os.getenv("BINANCE_SECRET")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID   = int(os.getenv("TG_CHAT_ID"))

PAIRS = json.load(open("params_multi.json"))
bot = Bot(token=TG_BOT_TOKEN)
dp = Dispatcher(bot)
exc = ccxt.binance({'apiKey': BINANCE_KEY, 'secret': BINANCE_SECRET, 'enableRateLimit': True})

# База Z для фильтрации "шумных" сигналов
last_z = shelve.open('last_z.shelve')

# Создать базу сделок (если не было)
def init_trades_db():
    conn = sqlite3.connect('trades.db')
    c = conn.cursor()
    c.execute('''
    CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_open TEXT,
        pair_a TEXT,
        pair_b TEXT,
        side_a TEXT,
        side_b TEXT,
        px_a REAL,
        px_b REAL,
        z_entry REAL,
        tp_a REAL,
        tp_b REAL,
        sl_a REAL,
        sl_b REAL,
        is_open INTEGER
    )
    ''')
    conn.commit()
    conn.close()
init_trades_db()

def get_last_prices(pair, window):
    db = duckdb.connect("data/quotes.duckdb")
    df_a = db.sql(f"SELECT ts, close FROM klines WHERE symbol='{pair['pair_a']}' AND interval='1d' ORDER BY ts DESC LIMIT {window+5}").df()
    df_b = db.sql(f"SELECT ts, close FROM klines WHERE symbol='{pair['pair_b']}' AND interval='1d' ORDER BY ts DESC LIMIT {window+5}").df()
    db.close()
    df_a = df_a.drop_duplicates(subset='ts', keep='last').set_index('ts')
    df_b = df_b.drop_duplicates(subset='ts', keep='last').set_index('ts')
    df = pd.concat([df_a, df_b], axis=1, keys=['a', 'b']).dropna().iloc[-window:]
    return df['a'].values, df['b'].values

async def monitor_pairs():
    while True:
        for pair in PAIRS:
            try:
                pA, pB = get_last_prices(pair, pair['window'])
                spread = np.log(pA[-1]) - np.log(pB[-1])
                hist_spread = np.log(pA) - np.log(pB)
                raw_z = (spread - hist_spread.mean()) / hist_spread.std()
                z = raw_z.item() if hasattr(raw_z, "item") else float(raw_z)
                pair_id = f"{pair['pair_a']}/{pair['pair_b']}"
                old_z = last_z.get(pair_id, None)

                if (old_z is None) or (abs(z - old_z) > 0.5):
                    now = datetime.now().strftime('%Y-%m-%d %H:%M')
                    if z > pair['zin']:
                        txt = (f"🔴 {now} {pair['pair_a']} vs {pair['pair_b']}\n"
                               f"Z={z:.2f}: Short {pair['pair_a']}, Long {pair['pair_b']}, lev={pair['lev']}")
                    elif z < -pair['zin']:
                        txt = (f"🟢 {now} {pair['pair_a']} vs {pair['pair_b']}\n"
                               f"Z={z:.2f}: Long {pair['pair_a']}, Short {pair['pair_b']}, lev={pair['lev']}")
                    else:
                        continue  # нет сигнала

                    markup = InlineKeyboardMarkup()
                    markup.add(InlineKeyboardButton(
                        "🔓 Открыть сделку",
                        callback_data=f"open|{pair['pair_a']}|{pair['pair_b']}|{z:.2f}|{pair['window']}"
                    ))
                    await bot.send_message(TG_CHAT_ID, txt, reply_markup=markup)
                    last_z[pair_id] = z
                    last_z.sync()
            except Exception as e:
                print(f"Error for {pair['pair_a']}/{pair['pair_b']}: {e}")
        await asyncio.sleep(1800)  # 30 минут

# Inline обработка кнопки "Открыть сделку"
@dp.callback_query_handler(lambda c: c.data.startswith('open|'))
async def open_trade_callback(callback_query: types.CallbackQuery):
    _, pair_a, pair_b, z_entry, window = callback_query.data.split('|')
    # Получить текущие цены
    pA, pB = get_last_prices(
        {'pair_a': pair_a, 'pair_b': pair_b, 'window': int(window)}, int(window)
    )
    px_a = float(pA[-1])
    px_b = float(pB[-1])
    # Рассчитать TP/SL (±2% для примера)
    tp_a, sl_a = px_a * 1.02, px_a * 0.98
    tp_b, sl_b = px_b * 0.98, px_b * 1.02
    side_a = "LONG" if float(z_entry) < 0 else "SHORT"
    side_b = "SHORT" if float(z_entry) < 0 else "LONG"
    # Запись в trades.db
    conn = sqlite3.connect('trades.db')
    c = conn.cursor()
    c.execute('''INSERT INTO trades
        (ts_open, pair_a, pair_b, side_a, side_b, px_a, px_b, z_entry, tp_a, tp_b, sl_a, sl_b, is_open)
        VALUES (datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)''',
        (pair_a, pair_b, side_a, side_b, px_a, px_b, z_entry, tp_a, tp_b, sl_a, sl_b))
    conn.commit()
    conn.close()
    await bot.answer_callback_query(callback_query.id, "Сделка сохранена!")
    await bot.send_message(
        TG_CHAT_ID,
        f"Сделка по {pair_a}/{pair_b} зарегистрирована\n"
        f"Тип: {side_a}/{side_b}\n"
        f"Цена открытия: {px_a:.4f}/{px_b:.4f}\n"
        f"TP: {tp_a:.4f}/{tp_b:.4f} | SL: {sl_a:.4f}/{sl_b:.4f}"
    )

# Команда: список открытых сделок
@dp.message_handler(commands=["open_trades"])
async def show_open_trades(message: types.Message):
    conn = sqlite3.connect('trades.db')
    c = conn.cursor()
    c.execute("SELECT id, pair_a, pair_b, px_a, px_b, z_entry, tp_a, tp_b, sl_a, sl_b, side_a, side_b FROM trades WHERE is_open=1")
    rows = c.fetchall()
    conn.close()
    if not rows:
        await message.answer("Открытых сделок нет.")
        return
    text = "\n\n".join([
        f"#{row[0]} {row[1]}/{row[2]} {row[10]}/{row[11]} Z={row[5]:.2f}\n"
        f"Открытие: {row[3]:.4f}/{row[4]:.4f}\n"
        f"TP: {row[6]:.4f}/{row[7]:.4f} | SL: {row[8]:.4f}/{row[9]:.4f}"
        for row in rows
    ])
    await message.answer("Открытые сделки:\n\n" + text)

# Запуск мониторинга
async def on_startup(dp):
    asyncio.create_task(monitor_pairs())

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
