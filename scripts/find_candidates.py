import duckdb, pandas as pd
con = duckdb.connect('data/quotes.duckdb')

def get_prices(symbol):
    return con.sql(f"""
      select ts, close from klines
      where symbol='{symbol}' and interval='1d'
      order by ts
    """).df().set_index('ts')['close']

symbols = [r[0] for r in con.sql("select distinct symbol from klines").fetchall()]
corrs=[]
for i,s1 in enumerate(symbols):
    p1 = get_prices(s1)
    for s2 in symbols[i+1:]:
        p2 = get_prices(s2)
        df = pd.concat([p1,p2],axis=1).dropna()
        if len(df)>90:
            cor = df.corr().iloc[0,1]
            if cor>0.6:
                corrs.append((s1,s2,cor,len(df)))
pd.DataFrame(corrs,columns=['sym1','sym2','corr','n']).to_csv('candidates.csv',index=False)
