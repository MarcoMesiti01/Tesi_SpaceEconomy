import pandas as pd
import Library as mylib

db_inv=mylib.openDB("investors")
db_round=mylib.openDB("rounds")

db_round=mylib.space(db_round, "company_id", True)
db_round=mylib.filterExits(db_round)

df_r=db_round[["investor_id", "investor_name", "round_amount_usd"]].groupby(by=["investor_id","investor_name"]).count()
df_r.sort_values(inplace=True, by="round_amount_usd", ascending=False)
#df_fin=pd.merge(left=df_r, right=db_inv, how="left", left_index=True, right_on="investor_id")
print(df_r)
df_r.head(30).to_excel("Output.xlsx")