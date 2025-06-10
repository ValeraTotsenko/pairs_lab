#!/usr/bin/env python
import duckdb, pandas as pd
from tqdm import tqdm

CORR_TH = 0.6            # порог корреляции
MIN_OVERLAP = 90         # мин. общих дней

db = duckdb.connect("data/quotes.duckdb")
symbols = [r[0] for r in db.sql("SELECT DISTINCT symbol FROM klines").fetchall()]

def price_series(sym):
    df = db.sql(f"""
        SELECT ts, close FROM klines
        WHERE symbol='{sym}' AND interval='1d'
        ORDER BY ts
    """).df()
    df = df.drop_duplicates(subset='ts', keep='last')
    return df.set_index('ts')['close']

for i, s1 in enumerate(tqdm(symbols, desc="corr scan")):
    p1 = price_series(s1)
    for s2 in symbols[i+1:]:
        p2 = price_series(s2)
        df = pd.concat([p1, p2], axis=1).dropna()
        if len(df) >= MIN_OVERLAP:
            c = df.corr().iloc[0, 1]
            if c > CORR_TH:
                pairs.append((s1, s2, c, len(df)))

pairs = []
for i, s1 in enumerate(tqdm(symbols, desc="corr scan")):
    p1 = price_series(s1).set_index('ts')['close']
    for s2 in symbols[i+1:]:
        p2 = price_series(s2).set_index('ts')['close']
        df = pd.concat([p1, p2], axis=1).dropna()
        if len(df) >= MIN_OVERLAP:
            c = df.corr().iloc[0, 1]
            if c > CORR_TH:
                pairs.append((s1, s2, c, len(df)))
pd.DataFrame(pairs, columns=['sym1', 'sym2', 'corr', 'n']) \
  .to_csv('candidates.csv', index=False)
print(f"✅ Найдено пар: {len(pairs)}")
