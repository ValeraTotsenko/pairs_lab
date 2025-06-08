import pandas as pd, numpy as np, duckdb, statsmodels.tsa.stattools as ts
from scipy import stats
from copulas.multivariate import StudentMultivariate

db = duckdb.connect('data/quotes.duckdb')
pairs = pd.read_csv('candidates.csv').head(20)  # берём топ-20

def price_series(sym):
    return db.sql(f"select ts, close from klines where symbol='{sym}' order by ts").df()

results=[]
for _,row in pairs.iterrows():
    p1 = price_series(row.sym1)
    p2 = price_series(row.sym2)
    df = p1.merge(p2, on='ts', suffixes=('1','2')).dropna()
    # 1) переводим в доходности
    r1 = df.close1.pct_change().dropna()
    r2 = df.close2.pct_change().dropna()
    # 2) ранги-квантили
    u = stats.rankdata(r1) / (len(r1)+1)
    v = stats.rankdata(r2) / (len(r2)+1)
    data = np.column_stack([u,v])
    # 3) фит copula
    cop = StudentMultivariate()
    cop.fit(data)
    rho = cop.covariance[0,1]
    # 4) лог-спред для ADF
    log_spread = np.log(df.close1/df.close2)
    adf_p = ts.adfuller(log_spread.dropna())[1]
    results.append((row.sym1,row.sym2,rho,adf_p))
pd.DataFrame(results, columns=['s1','s2','rho','adf_p']).to_csv('pairs_ready.csv',index=False)
