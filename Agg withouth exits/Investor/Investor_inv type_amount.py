import pandas as pd
import Library as mylib

db_inv=pd.read_parquet("DB_Out/DB_investors.parquet")
db_round=pd.read_parquet("DB_Out/RoundSplit.parquet")
db_exp=pd.read_parquet("DB_Out/DB_export.parquet", columns=["company_id","company_all_tags"])
db_exp=mylib.space(db_exp)
db_exp=db_exp["company_id"]
db_round=db_round[db_round["Target firm ID"].isin(db_exp)]
db_round=mylib.filterExits(db_round)
df_round=db_round[["Investor ID","Investor", "AmountUSD"]].groupby(by=["Investor ID", "Investor"]).sum()
df_round.reset_index(inplace=True)
df_fin=pd.merge(left=df_round, right=db_inv, how="left", left_on="Investor ID", right_on="ID")
df_fin.sort_values(by="AmountUSD", inplace=True, ascending=False)
print(df_fin)
df_fin=df_fin[["Investor_x", "Investor type", "AmountUSD"]].head(30)
df_fin["AmountUSD"]=df_fin["AmountUSD"].apply(lambda x: x/1000000000 if not pd.isna(x) else x)
print(df_fin["Investor type"].to_list())
df_fin.to_excel("Output.xlsx")