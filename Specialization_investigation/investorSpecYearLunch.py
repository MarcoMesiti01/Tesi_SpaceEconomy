import pandas as pd
import Library as mylib

df=pd.read_parquet("DB_Out/Fact/FactInvestorYearSpecialization.parquet")

dfInv=mylib.openDB("investors")[["investor_id","investor_launch_year"]]

df=pd.merge(left=df, right=dfInv, how="inner", on="investor_id")

#filtering only on VCs that have at least 4 deals
rounds=mylib.openDB("rounds")[["investor_id","round_amount_usd"]]
rounds=rounds.groupby(by="investor_id").count()
rounds=rounds[rounds["round_amount_usd"]>=4]

df=pd.merge(left=df, right=rounds, left_on="investor_id", right_index=True, how="inner")

df.to_excel("Output.xlsx")

