import pandas as pd, numpy as np, vectorbt as vbt, pymoo
from pymoo.algorithms.moo.nsga2 import NSGA2
from pymoo.factory import get_problem, get_sampling, get_crossover, get_mutation
from pymoo.optimize import minimize

pair = pd.read_csv('pairs_ready.csv').iloc[0]   # пример: первая пара

# ------- подготовка цен ---------
def prices(sym):
    return vbt.YFData.download(sym.replace('/USDT','-USDT'), period='720d').get('Close')
p1 = prices(pair.s1); p2 = prices(pair.s2)
close = pd.concat({'a': p1, 'b': p2}, axis=1).dropna()

# ------- проблема оптимизации -----
class PairProblem(get_problem("none")):
    def __init__(self, close):
        self.close = close
        super().__init__(n_var=4, n_obj=2, xl=[20,1,0.5,1], xu=[60,3,1,3])
    def _evaluate(self, X, out, *args, **kwargs):
        rets, dd = [], []
        for w_gap, z_in, z_out, lev in X:
            spread = np.log(self.close.a) - np.log(self.close.b)
            m = spread.rolling(int(w_gap)).mean()
            s = spread.rolling(int(w_gap)).std()
            z = (spread - m)/s
            longs = (z < -z_in).astype(int)
            shorts= (z >  z_in).astype(int)*-1
            pos   = (longs+shorts).ffill().fillna(0)*lev
            pnl   = pos.shift()*((self.close.pct_change()).sum(axis=1))
            equity= (1+pnl.fillna(0)).cumprod()
            rets.append(-equity.iloc[-1])          # цель 1: −CAGR
            dd.append(equity.min()/equity.max())   # цель 2: MaxDD ratio
        out["F"] = np.column_stack([rets, dd])

problem = PairProblem(close)
res = minimize(problem, NSGA2(pop_size=40,
            sampling=get_sampling("real_random"),
            crossover=get_crossover("real_sbx", prob=0.9, eta=15),
            mutation=get_mutation("real_pm", eta=20),
            eliminate_duplicates=True),
        ('n_gen', 50), verbose=False)

opt = res.X[np.argmin([f[0] for f in res.F])]
print("Оптимальные параметры:", opt)
