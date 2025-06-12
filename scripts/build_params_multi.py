#!/usr/bin/env python3
import json, numpy as np, pandas as pd, duckdb, sys
from pymoo.algorithms.moo.nsga2 import NSGA2
from pymoo.factory import get_sampling, get_crossover, get_mutation
from pymoo.optimize import minimize

def prices(sym):
    db = duckdb.connect("data/quotes.duckdb")
    df = db.sql(f"""SELECT ts, close FROM klines
                    WHERE symbol='{sym}' AND interval='1d'
                    ORDER BY ts""").df().drop_duplicates(subset='ts', keep='last')
    db.close()
    return pd.Series(df['close'].values, index=pd.to_datetime(df['ts']))

def optimize_params(sym_a, sym_b):
    close_a = prices(sym_a)
    close_b = prices(sym_b)
    df = pd.concat([close_a, close_b], axis=1, keys=['a', 'b']).dropna()
    if len(df) < 120:
        print(f"Недостаточно данных для {sym_a} и {sym_b}")
        return None

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
                ret = -equity.iloc[-1]
                dd = equity.min() / equity.max()
                Fs.append([ret, dd])
            return np.array(Fs)

    problem = PairProblem(df)

    res = minimize(
        problem,
        NSGA2(pop_size=20,
              sampling=get_sampling("real_random"),
              crossover=get_crossover("real_sbx", prob=0.9, eta=15),
              mutation=get_mutation("real_pm", eta=20)),
        ('n_gen', 20),
        verbose=False
    )

    scores = res.F
    best_i = np.lexsort((scores[:, 1], scores[:, 0]))[0]
    best = res.X[best_i].round(3).tolist()
    return {
        "pair_a": sym_a,
        "pair_b": sym_b,
        "window": int(best[0]),
        "zin": best[1],
        "zout": best[2],
        "lev": best[3]
    }

if __name__ == "__main__":
    # Считываем пары
    pairs = pd.read_csv('pairs_ready.csv')
    params_list = []

    for i, row in pairs.iterrows():
        print(f"[{i+1}/{len(pairs)}] Оптимизация для {row.s1} и {row.s2}...")
        result = optimize_params(row.s1, row.s2)
        if result is not None:
            params_list.append(result)

    # Сохраняем всё сразу
    with open("params_multi.json", "w") as f:
        json.dump(params_list, f, indent=2, ensure_ascii=False)
    print(f"\n✅ Сохранено {len(params_list)} оптимальных конфигов в params_multi.json")
