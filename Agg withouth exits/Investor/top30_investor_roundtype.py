import pandas as pd
import Library as mylib
import matplotlib.pyplot as plt

db_round=pd.read_parquet("DB_Out/RoundSplit.parquet")
df_inv=pd.read_parquet("DB_Out/DB_investors.parquet")
db_exp=pd.read_parquet("DB_Out/DB_export.parquet", columns=["company_id","company_all_tags"])
db_exp=mylib.space(db_exp)
db_exp=db_exp["company_id"]
df=db_round[db_round["Target firm ID"].isin(db_exp)]

df_merge=pd.merge(left=df, right=df_inv, how="left", left_on="Investor ID", right_on="ID")

df_merge=mylib.filterExits(df_merge)

df_merge_inv_rt=df_merge[["Investor ID", "Investor_x", "Round type", "AmountUSD"]]
df_merge_inv_rt.astype({"Investor ID" : "int", "Investor_x" : "string", "Round type" : "string", "AmountUSD" : "float"})
df_merge_inv_rt=df_merge_inv_rt.groupby(by=["Investor ID", "Investor_x", "Round type"]).sum()
df_merge_inv_rt.reset_index(inplace=True)
df_merge_inv_rt.columns=["Investor ID", "Investor_x", "Round type", "sum"]
df_merge_inv_rt.sort_values(by="sum", inplace=True, ascending=False)
df_merge_inv_rt.head(30).to_excel("Output.xlsx")
print(df_merge)

df_rt=df_merge[["Round type", "AmountUSD"]]
print(df_rt.columns)
print(df_rt)
df_rt["AmountUSD"]=pd.to_numeric(df_rt["AmountUSD"], errors="coerce")
df_rt=df_rt.groupby(by="Round type", group_keys=False).sum()
print(df_rt)
df_rt.reset_index(inplace=True)
df_rt=df_rt[["Round type", "AmountUSD"]]
df_rt.columns=["Round type", "sum"]
df_rt.sort_values(by="sum", inplace=True)
#df_rt.mask(df_rt["Round type"]=="PROJECT, REAL ESTATE, INFRASTRUCTURE FINANCE", other="PROJ, RE, IF", inplace=True)
df_rt.at[16, "Round type"]="PROJ, RE, IF"
print(df_rt.columns)
print(df_rt.loc[4])
plt.bar(df_rt["Round type"].tail(18), df_rt["sum"].tail(18))
plt.xlabel("Round type")
plt.tick_params(axis="x", rotation=90)
plt.ylabel("Amount invested M EUR")
plt.title("Amount invested per round type")
plt.show()

df_rt=df_merge[["Round type", "AmountUSD"]]
df_rt=df_rt.groupby(by="Round type").count()
print(df_rt)
df_rt.reset_index(inplace=True)
df_rt.columns=["Round type", "count"]
df_rt.sort_values(by="count", inplace=True)
plt.bar(df_rt["Round type"].tail(18), df_rt["count"].tail(18))
plt.xlabel("Round type")
plt.tick_params(axis="x", rotation=90)
plt.ylabel("Number of rounds")
plt.title("Number of rounds per round type")
plt.show()