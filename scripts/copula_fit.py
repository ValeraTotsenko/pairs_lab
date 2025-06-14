#!/usr/bin/env python3
import duckdb, pandas as pd, numpy as np
import statsmodels.tsa.stattools as ts
from tqdm import tqdm

cand = pd.read_csv('candidates.csv')
good = []
db   = duckdb.connect("data/quotes.duckdb")

def price(sym):
    df = db.sql(f"""
        SELECT ts, close FROM klines
        WHERE symbol='{sym}' AND interval='1d'
        ORDER BY ts
    """).df()
    df = df.drop_duplicates(subset='ts', keep='last')
    return pd.Series(df['close'].values, index=df['ts'])

for _, row in tqdm(cand.iterrows(), total=len(cand)):
    p1 = price(row.sym1)
    p2 = price(row.sym2)
    df = pd.concat([p1, p2], axis=1, keys=['close1', 'close2']).dropna()
    if len(df) < 120:
        continue
    # Используем доходности
    r1 = df['close1'].pct_change().dropna()
    r2 = df['close2'].pct_change().dropna()
    # Корреляция Спирмена
    rho = r1.corr(r2, method='spearman')
    try:
        adf_p = ts.adfuller(np.log(df['close1'] / df['close2']))[1]
        if abs(rho) > 0.6 and adf_p < 0.05:
            good.append((row.sym1, row.sym2, rho, adf_p))
    except Exception as e:
        print(f"skip {row.sym1}/{row.sym2} — ADF fail: {e}")

pd.DataFrame(good, columns=['s1', 's2', 'rho', 'adf_p']) \
  .to_csv('pairs_ready.csv', index=False)
print(f"✅ Отобрано пар: {len(good)}")
