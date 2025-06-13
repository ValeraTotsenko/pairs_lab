# scripts/init_trades_db.py
import sqlite3

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
print("Таблица сделок создана (или уже была)")
