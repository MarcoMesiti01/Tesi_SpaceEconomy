import pandas as pd
import Library as mylib

db_round=pd.read_parquet("DB_Out/RoundSplit.parquet")

df_ex=db_round["Amount in EUR"]
df_noex=mylib.filterExits(db_round)["Amount in EUR"]
df_noex=pd.to_numeric(df_noex)
df_noex=df_noex.sum()
print(df_noex)
df_ex=pd.to_numeric(df_ex)
df_ex=df_ex.sum()
print(df_ex)