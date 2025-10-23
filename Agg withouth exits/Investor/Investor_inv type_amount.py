import pandas as pd
import Library as mylib

db_inv=pd.read_parquet("DB_Out/DB_investors.parquet")
db_round=pd.read_parquet("DB_Out/RoundSplit.parquet")
db_exp=pd.read_parquet("DB_Out/DB_export.parquet", columns=["company_id","company_all_tags"])
db_exp=mylib.space(db_exp)
db_exp=db_exp["company_id"]
db_round=db_round[db_round["company_id"].isin(db_exp)]
db_round=mylib.filterExits(db_round)
df_round=db_round[["investor_id","Investor", "round_amount_usd"]].groupby(by=["investor_id", "Investor"]).sum()
df_round.reset_index(inplace=True)
df_fin=pd.merge(left=df_round, right=db_inv, how="left", left_on="investor_id", right_on="ID")
df_fin.sort_values(by="round_amount_usd", inplace=True, ascending=False)
print(df_fin)
df_fin=df_fin[["Investor_x", "investor_types", "round_amount_usd"]].head(30)
df_fin["round_amount_usd"]=df_fin["round_amount_usd"].apply(lambda x: x/1000000000 if not pd.isna(x) else x)
print(df_fin["investor_types"].to_list())
df_fin.to_excel("Output.xlsx")