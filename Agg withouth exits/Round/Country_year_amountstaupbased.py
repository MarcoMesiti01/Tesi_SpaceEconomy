import pandas as pd
import Library as mylib
import numpy as np
import matplotlib.pyplot as plt

#countryList=["Germany", "Italy", "United States", "France", "United Kingdom", "China"]
df_round=pd.read_parquet("DB_Out/RoundSplit.parquet")
db_exp=pd.read_parquet("DB_Out/DB_export.parquet", columns=["company_id","company_all_tags"])
db_exp=mylib.space(db_exp)
db_exp=db_exp["company_id"]
df_round=df_round[df_round["Target firm ID"].isin(db_exp)]
df_round=mylib.filterExits(df_round)
df_round=df_round[["Firm country", "Round date", "AmountUSD"]]
df_round.rename(columns={"Firm country":"Country"}, inplace=True)
df_round=mylib.toEU(df_round)
df_round.rename(columns={"Country":"Firm country"}, inplace=True)
df_round["AmountUSD"]=df_round["AmountUSD"]/1000000000
df_top_c=df_round[["Firm country", "AmountUSD"]].groupby("Firm country").sum()
df_top_c.reset_index(inplace=True)
df_top_c.sort_values(by="AmountUSD", inplace=True, ascending=False)
top_c=df_top_c["Firm country"].head(5).to_list()
df_round["Round date"]=df_round["Round date"].apply(mylib.getYear, by_row="compat")
df_round=df_round[df_round["Round date"]>2010]
df_round=df_round[df_round["Firm country"].isin(top_c)]
df_round=df_round.groupby(by=["Firm country", "Round date"], sort=True).sum()
#df_round.reset_index(inplace=True)
#df_round.sort_values(by="AmountUSD", inplace=True, ascending=False)
df_round=df_round.groupby("Firm country")
filled_markers = ['o', 'v', '^', '<', '>', '8', 's', 'p', '*', 'h', 'H', 'D', 'd', 'P', 'X']
j=0
plt.figure(figsize=(8,5))
for x,y in df_round:
    years=y.index.get_level_values("Round date")
    plt.plot(years, y["AmountUSD"], marker=filled_markers[j], label=x)
    j+=1
    if j>=5:
        break
plt.title("Main countries space investment over time")
plt.xlabel("Year")
plt.ylabel("Value")
plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
plt.grid(True)
plt.show()
print(df_round)