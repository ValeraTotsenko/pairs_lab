#!/usr/bin/env python
"""
Оптимизирует параметры Z-стратегии для первой пары из pairs_ready.csv
или для пары, переданной через --pair "UNI/USDT:SUSHI/USDT"
"""
import argparse, json, numpy as np, pandas as pd, datetime as dt
from pymoo.algorithms.moo.nsga2 import NSGA2
from pymoo.factory     import get_sampling, get_crossover, get_mutation
from pymoo.optimize    import minimize
import vectorbt as vbt

# ---------- CLI ----------
p = argparse.ArgumentParser()
p.add_argument("--pair", help="custom pair format A/B:C/D")
args = p.parse_args()

if args.pair:
    sym_a, sym_b = args.pair.split(":")
else:
    pair = pd.read_csv('pairs_ready.csv').iloc[0]
    sym_a, sym_b = pair.s1, pair.s2

print(f"🔧 Оптимизация для {sym_a} vs {sym_b}")

# ---------- Цены ----------
def prices(sym):
    return vbt.CCXTData.download(sym, exchange='binance',
                                 timeframe='1d', start=dt.date(2023,1,1)).get('Close')
close = pd.concat({'a': prices(sym_a), 'b': prices(sym_b)}, axis=1).dropna()

# ---------- NSGA ----------
class PairProblem:
    def __init__(self, close):
        self.close = close
        self.n_var, self.n_obj = 4, 2
        self.xl = np.array([20, 1.0, 0.5, 1])
        self.xu = np.array([60, 3.0, 1.0, 3])
    def evaluate(self, X):
        Fs = []
        for w, zin, zout, lev in X:
            spread = np.log(self.close.a) - np.log(self.close.b)
            m  = spread.rolling(int(w)).mean()
            sd = spread.rolling(int(w)).std()
            z  = (spread - m) / sd
            entry  = (z >  zin) * -1 + (z < -zin) * 1
            exit_  = (z.abs() < zout).astype(int)
            pos    = entry.replace(0, np.nan).ffill().where(exit_==0).fillna(0) * lev
            pnl    = pos.shift() * self.close.pct_change().sum(axis=1)
            equity = (1 + pnl.fillna(0)).cumprod()
            ret    = -equity.iloc[-1]             # мин. (–CAGR)
            dd     = equity.min() / equity.max()  # мин. draw-ratio
            Fs.append([ret, dd])
        return np.array(Fs)

problem = PairProblem(close)

res = minimize(
    problem,
    NSGA2(pop_size=40,
          sampling=get_sampling("real_random"),
          crossover=get_crossover("real_sbx", prob=0.9, eta=15),
          mutation=get_mutation("real_pm", eta=20)),
    ('n_gen', 50),
    verbose=False
)
# выбрать особь с макс. -ret (т.е. макс. equity) и мин. dd
scores = res.F
best_i = np.lexsort((scores[:,1], scores[:,0]))[0]
best   = res.X[best_i].round(3).tolist()
print("✓ best params:", best)

json.dump({
    "pair_a": sym_a, "pair_b": sym_b,
    "window": int(best[0]), "zin": best[1],
    "zout": best[2], "lev": best[3]
}, open("params.json", "w"), indent=2)
