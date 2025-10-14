import pandas as pd
import openpyxl
import Library as mylib
import json 

"""df=pd.read_parquet("DB_temp.parquet")
L=list()
round=mylib.roundSplit(L, df)
with open("Aggregations/conversion_rate.json", "r") as f:
    conversion_dict=json.load(f)
f.close()
round=mylib.amountsConv(round, conversion_dict)
print(len(round))
round.to_excel("OutputTest.xlsx")
round.to_parquet(path="DB_Out/RoundSplit.parquet")"""

def compute(x):
    if x["round_amount_usd"]==0:
        return 0
    elif x["total_investors"]==0:
        return 0
    else:
        return x["round_amount_usd"]/x["total_investors"]

df=pd.read_parquet("DB_Out/DB_export.parquet")
df["round_amount_usd"]=df["round_amount_usd"].fillna(0)
df["total_investors"]=df["total_investors"].fillna(0)
df["round_amount_usd"]=df.apply(lambda x: compute(x), axis=1)
df_round=df[["investor_id","investor_name","round_amount_usd","round_label","round_date","company_id","company_name","company_country"]]
df_round.columns=["Investor ID","Investor","AmountUSD","Round type","Round date","Target firm ID","Target firm","Firm country"]
lisAm=df_round["AmountUSD"].tolist()
lisAm.sort()
print(lisAm)
df_round.to_parquet("DB_Out/RoundSplit.parquet")
