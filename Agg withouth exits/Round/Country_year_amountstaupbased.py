import pandas as pd
import Library as mylib
import numpy as np
import matplotlib.pyplot as plt

# Increase default font sizes for readability
plt.rcParams.update({
    'font.size': 20,
    'axes.titlesize': 20,
    'axes.labelsize': 20,
    'xtick.labelsize': 20,
    'ytick.labelsize': 20,
    'legend.fontsize': 20,
})

#countryList=["Germany", "Italy", "United States", "France", "United Kingdom", "China"]
df_round=mylib.openDB("rounds")
db_exp=pd.read_parquet("DB_Out/DB_export.parquet", columns=["company_id","company_all_tags"])
db_exp=mylib.space(db_exp, "company_id", True)
db_exp=db_exp["company_id"]
df_round=df_round[df_round["company_id"].isin(db_exp)]
df_round=mylib.filterExits(df_round)
df_round=df_round[["company_country", "round_date", "round_amount_usd"]]
df_round.rename(columns={"company_country":"Country"}, inplace=True)
df_round=mylib.toEU(df_round)
df_round.rename(columns={"Country":"company_country"}, inplace=True)
df_round["round_amount_usd"]=df_round["round_amount_usd"]/1000000000
df_top_c=df_round[["company_country", "round_amount_usd"]].groupby("company_country").sum()
df_top_c.reset_index(inplace=True)
df_top_c.sort_values(by="round_amount_usd", inplace=True, ascending=False)
top_c=df_top_c["company_country"].head(5).to_list()
df_round["round_date"]=df_round["round_date"].apply(mylib.getYear, by_row="compat")
df_round=df_round[df_round["round_date"]>=2010]
df_round=df_round[df_round["company_country"].isin(top_c)]
df_round=df_round.groupby(by=["company_country", "round_date"], sort=True).sum()
#df_round.reset_index(inplace=True)
#df_round.sort_values(by="round_amount_usd", inplace=True, ascending=False)
df_round=df_round.groupby("company_country")
filled_markers = ['o', 'v', '^', '<', '>', '8', 's', 'p', '*', 'h', 'H', 'D', 'd', 'P', 'X']
j=0
plt.figure(figsize=(8,5))
for x,y in df_round:
    years=y.index.get_level_values("round_date")
    plt.plot(years, y["round_amount_usd"], marker=filled_markers[j], label=x)
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
