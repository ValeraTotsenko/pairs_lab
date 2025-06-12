#!/usr/bin/env python3
import duckdb
import pandas as pd
from tqdm import tqdm

# ——— Параметры фильтрации ———
CORR_TH = 0.6             # Порог корреляции
MIN_OVERLAP = 120         # Минимальное пересечение истории (дней)
MIN_VOL = 20_000          # Минимальный средний объём за 90 дней ($)
TOP_N = 100               # Сколько топ-пар отдать в output

db = duckdb.connect("data/quotes.duckdb")

# 1. Собираем список ликвидных монет
symbols = []
for row in db.sql("SELECT DISTINCT symbol FROM klines").fetchall():
    sym = row[0]
    df = db.sql(f"SELECT ts, close, volume FROM klines WHERE symbol='{sym}' AND interval='1d' ORDER BY ts").df()
    df = df.drop_duplicates(subset='ts', keep='last')
    if len(df) < MIN_OVERLAP:
        continue
    avg_vol = df['volume'][-90:].mean()
    if avg_vol < MIN_VOL:
        continue
    symbols.append(sym)

print(f"Используем ликвидных монет: {len(symbols)}")

# 2. Ищем пары и считаем корреляции
pairs = []
for i, s1 in enumerate(tqdm(symbols, desc="corr scan")):
    df1 = db.sql(f"SELECT ts, close FROM klines WHERE symbol='{s1}' AND interval='1d' ORDER BY ts").df()
    df1 = df1.drop_duplicates(subset='ts', keep='last')
    p1 = pd.Series(df1['close'].values, index=df1['ts'])
    for s2 in symbols[i+1:]:
        df2 = db.sql(f"SELECT ts, close FROM klines WHERE symbol='{s2}' AND interval='1d' ORDER BY ts").df()
        df2 = df2.drop_duplicates(subset='ts', keep='last')
        p2 = pd.Series(df2['close'].values, index=df2['ts'])
        df = pd.concat([p1, p2], axis=1, keys=['close1', 'close2']).dropna()
        if len(df) < MIN_OVERLAP:
            continue
        c = df.corr().iloc[0, 1]
        if c > CORR_TH:
            pairs.append((s1, s2, c, len(df)))

# 3. Сохраняем top-N по корреляции
df_pairs = pd.DataFrame(pairs, columns=['sym1', 'sym2', 'corr', 'n'])
df_pairs = df_pairs.sort_values('corr', ascending=False).head(TOP_N)
df_pairs.to_csv('candidates.csv', index=False)
print(f"✅ Сохранил {len(df_pairs)} top-пар из {len(pairs)} возможных")
