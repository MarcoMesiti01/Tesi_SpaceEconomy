import pandas as pd
import Library as mylib
import numpy as np

dfInv=pd.read_parquet("DB_Out/AmountbyInvType.parquet")
dfLoc=pd.read_parquet("DB_Out/Inv_Loc_finale.parquet")
dfFin=pd.merge(dfInv, dfLoc, on="Investor", how="left")
dfFin.sort_values(by="Amount in EUR", inplace=True, ascending=False)   
dfFin=mylib.polish_loc(dfFin)
dfFin.to_excel("Output.xlsx")
print(dfFin.head())
dfMap=dfFin[["country", "Amount in EUR"]]
dfMap=dfMap.groupby(by="country").sum()
dfMap.reset_index(inplace=True)
dfMap.rename(columns={"country" : "Firm country", "Amount in EUR" : "Total amount"}, inplace=True)
dfMap.fillna("", inplace=True)
fig=mylib.makeMap(dfMap, "Total amount")
fig.write_html("InvestorTotalAmountCountry.html")

"""dfMap=dfFin[["country", "Investor"]]
dfMap=dfMap.groupby(by="country").count()
dfMap.reset_index(inplace=True)
dfMap.rename(columns={"country" : "Firm country", "Investor" : "Number of investor"}, inplace=True)
dfMap.fillna("", inplace=True)
fig=mylib.makeMap(dfMap, "Number of investor")
fig.write_html("NumberofInvestorsCountries.html")"""
