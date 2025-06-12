#!/usr/bin/env python3
"""
Оптимизирует параметры Z-стратегии для выбранной пары из pairs_ready.csv
или для пары, переданной через --pair "A/USDT:B/USDT"
"""
import argparse, json, numpy as np, pandas as pd, datetime as dt
from pymoo.algorithms.moo.nsga2 import NSGA2
from pymoo.factory import get_sampling, get_crossover, get_mutation
from pymoo.optimize import minimize
import vectorbt as vbt

# ---------- CLI ----------
p = argparse.ArgumentParser()
p.add_argument("--pair", help="custom pair format A/USDT:B/USDT")
args = p.parse_args()

# ---------- Загрузка пар ----------
if args.pair:
    sym_a, sym_b = args.pair.split(":")
else:
    pairs = pd.read_csv('pairs_ready.csv')
    sym_a, sym_b = pairs.s1.iloc[0], pairs.s2.iloc[0]

print(f"🔧 Оптимизация для {sym_a} vs {sym_b}")

# ---------- Загрузка цен ----------
def prices(sym):
    df = pd.read_csv('data/klines.csv') if False else None
    # Можно переписать если ценовые данные лежат в CSV, но для DuckDB лучше:
    import duckdb
    db = duckdb.connect("data/quotes.duckdb")
    q = f"""SELECT ts, close FROM klines
            WHERE symbol='{sym}' AND interval='1d'
            ORDER BY ts"""
    df = db.sql(q).df().drop_duplicates(subset='ts', keep='last')
    ser = pd.Series(df['close'].values, index=pd.to_datetime(df['ts']))
    return ser

close_a = prices(sym_a)
close_b = prices(sym_b)
df = pd.concat([close_a, close_b], axis=1, keys=['a', 'b']).dropna()
if len(df) < 120:
    raise ValueError("Недостаточно данных для выбранной пары.")

# ---------- NSGA Оптимизация ----------
class PairProblem:
    def __init__(self, close):
        self.close = close
        self.n_var, self.n_obj = 4, 2
        self.xl = np.array([20, 1.0, 0.5, 1.0])
        self.xu = np.array([60, 3.0, 1.5, 3.0])  # окно, zin, zout, плечо

    def evaluate(self, X):
        Fs = []
        for w, zin, zout, lev in X:
            spread = np.log(self.close.a) - np.log(self.close.b)
            m = spread.rolling(int(w)).mean()
            sd = spread.rolling(int(w)).std()
            z = (spread - m) / sd
            entry = (z > zin) * -1 + (z < -zin) * 1
            exit_ = (z.abs() < zout).astype(int)
            pos = entry.replace(0, np.nan).ffill().where(exit_==0).fillna(0) * lev
            pnl = pos.shift() * (self.close.a.pct_change() - self.close.b.pct_change())
            equity = (1 + pnl.fillna(0)).cumprod()
            ret = -equity.iloc[-1]            # минимизируем минус доходность
            dd = equity.min() / equity.max()  # минимизируем max drawdown
            Fs.append([ret, dd])
        return np.array(Fs)

problem = PairProblem(df)

res = minimize(
    problem,
    NSGA2(pop_size=40,
          sampling=get_sampling("real_random"),
          crossover=get_crossover("real_sbx", prob=0.9, eta=15),
          mutation=get_mutation("real_pm", eta=20)),
    ('n_gen', 50),
    verbose=True
)

scores = res.F
best_i = np.lexsort((scores[:, 1], scores[:, 0]))[0]
best = res.X[best_i].round(3).tolist()
print("✓ best params:", best)

with open("params.json", "w") as f:
    json.dump({
        "pair_a": sym_a, "pair_b": sym_b,
        "window": int(best[0]), "zin": best[1],
        "zout": best[2], "lev": best[3]
    }, f, indent=2)
print("✅ Сохранил параметры в params.json")
