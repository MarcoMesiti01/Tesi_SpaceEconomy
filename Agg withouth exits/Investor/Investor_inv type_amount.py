import pandas as pd
import Library as mylib

db_inv=pd.read_parquet("DB_Out/DB_investors.parquet")
db_round=pd.read_parquet("DB_Out/DB_rounds.parquet")
db_round=mylib.space(db_round, "company_id",True)
db_round=mylib.filterExits(db_round)
db_inv=mylib.isOriginalVC(db_inv, True)
print(len(db_inv))
df_round=db_round[["investor_id","investor_name", "round_amount_usd"]].groupby(by=["investor_id", "investor_name"]).sum()
df_round.reset_index(inplace=True)

#filtering the investor df
#db_inv=db_inv[(~db_inv["investor_types"].str.contains("Venture capital", case=False, na=False)) & (~db_inv["investor_types"].str.contains("venture_capital", case=False, na=False)) & (~db_inv["investor_types"].str.contains("Not defined", case=False, na=False))]
df_fin=pd.merge(left=df_round, right=db_inv[["investor_id","investor_types"]], how="inner", on="investor_id")
df_fin.sort_values(by="round_amount_usd", inplace=True, ascending=False)
print(df_fin)
df_fin=df_fin[["investor_name", "investor_types", "round_amount_usd"]].head(30)
df_fin["round_amount_usd"]=df_fin["round_amount_usd"].apply(lambda x: x/1000000000 if not pd.isna(x) else x)
print(df_fin["investor_types"].to_list())
df_fin.to_excel("Output.xlsx")