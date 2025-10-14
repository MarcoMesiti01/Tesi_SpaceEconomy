import pandas as pd
import Library as mylib

db_inv=pd.read_parquet("DB_Out/DB_investors.parquet")
db_round=pd.read_parquet("DB_Out/RoundSplit.parquet")
db_exp=pd.read_parquet("DB_Out/DB_export.parquet", columns=["company_id","company_all_tags"])
db_exp=mylib.space(db_exp)
db_exp=db_exp["company_id"]
db_round=db_round[db_round["Target firm ID"].isin(db_exp)]

db_round=mylib.filterExits(db_round)

df_r=db_round[["Investor ID", "Investor", "AmountUSD"]].groupby(by=["Investor ID","Investor"]).count()
df_r.reset_index(inplace=True)
df_r.sort_values(inplace=True, by="AmountUSD", ascending=False)
df_fin=pd.merge(left=df_r, right=db_inv, how="left", left_on="Investor ID", right_on="ID")
print(df_fin)
df_fin.head(30).to_excel("Output.xlsx")