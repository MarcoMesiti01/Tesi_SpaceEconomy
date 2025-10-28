import pandas as pd
import Library as mylib



dfround=mylib.openDB("rounds")
dfinv=mylib.openDB("investors")
dfround=pd.merge(left=dfround, right=dfinv, left_on="investor_id", right_on="investor_id", how="left")
dfamount=dfround[["investor_id","round_amount_usd"]].groupby(by="investor_id").sum()
dfamount.columns=["investor_total_amount"]
dfround=pd.merge(left=dfround, right=dfamount, left_on="investor_id", how="left", right_index=True)
print(dfround)
dfamountCountry=dfround[["investor_id","round_amount_usd","investor_country","company_country"]]
dfamountCountry=dfamountCountry[dfamountCountry["investor_country"]==dfamountCountry["company_country"]].copy()
dfamountCountry=dfamountCountry[["investor_id","round_amount_usd"]].groupby(by="investor_id").sum()
dfamountCountry.columns=["amount_same_country"]
dfround=pd.merge(left=dfround, right=dfamountCountry, left_on="investor_id", right_index=True, how="left")
dfround=dfround.mask((dfround["amount_same_country"]==0) | (pd.isna(dfround["amount_same_country"])), other=0)
dfround["percentage"]=dfround["amount_same_country"]/dfround["investor_total_amount"]
print(dfround["percentage"].mean())