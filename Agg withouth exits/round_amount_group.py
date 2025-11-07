import pandas as pd
import Library as mylib
import matplotlib.pyplot as plt
import numpy as np

# Increase default font sizes for readability
plt.rcParams.update({
    'font.size': 14,
    'axes.titlesize': 18,
    'axes.labelsize': 14,
    'xtick.labelsize': 12,
    'ytick.labelsize': 12,
    'legend.fontsize': 12,
})

df=mylib.openDB("rounds")
db_exp=pd.read_parquet("DB_Out/DB_export.parquet", columns=["company_id","company_all_tags"])
db_exp=mylib.space(db_exp, "company_id", True)
db_exp=db_exp["company_id"]
df=df[df["company_id"].isin(db_exp)]

df=df[df["round_amount_usd"]!=0]
df["round_amount_usd"]=df["round_amount_usd"].apply(lambda x: x/1000000 if not pd.isna(x) else x)
df=mylib.filterExits(df)
df=df[df["round_label"]!="NULL"]
df_sort=df.sort_values(by="round_amount_usd", ascending=False)
df_sort=df_sort[["investor_id", "round_amount_usd"]]
print(df_sort[:10])
df_onemln=df[df["round_amount_usd"]<1]
df_threemln=df[(df["round_amount_usd"]>=1) & (df["round_amount_usd"]<3)]
df_fivemln=df[(df["round_amount_usd"]>=3) & (df["round_amount_usd"]<5)]
df_tenmln=df[(df["round_amount_usd"]<10) & (df["round_amount_usd"]>=5)]
df_tentwentymln=df[(df["round_amount_usd"]>=10) & (df["round_amount_usd"]<20)]
df_morethantwenty=df[(df["round_amount_usd"]>=20) & (df["round_amount_usd"]<50)]
df_morethanfifthy=df[(df["round_amount_usd"]>=50) & (df["round_amount_usd"]<200)]
df_morethantwohundred=df[df["round_amount_usd"]>=200]

listSizes=[df_onemln.size, df_threemln.size, df_fivemln.size, df_tenmln.size, df_tentwentymln.size, df_morethantwenty.size, df_morethanfifthy.size, df_morethantwohundred.size]
print(listSizes)
label=["<1", "1-3", "3-5", "5-10", "10-20", "20-50", "50-200", ">200"]
df_onemln=df_onemln["round_amount_usd"].sum()
df_threemln=df_threemln["round_amount_usd"].sum()
df_fivemln=df_fivemln["round_amount_usd"].sum()
df_tenmln=df_tenmln["round_amount_usd"].sum()
df_tentwentymln=df_tentwentymln["round_amount_usd"].sum()
df_morethantwenty=df_morethantwenty["round_amount_usd"].sum()
df_morethanfifthy=df_morethanfifthy["round_amount_usd"].sum()
df_morethantwohundred=df_morethantwohundred["round_amount_usd"].sum()
amountSums=[df_onemln, df_threemln, df_fivemln, df_tenmln, df_tentwentymln, df_morethantwenty, df_morethanfifthy, df_morethantwohundred]
plt.bar(label, listSizes)
plt.xlabel("Round amount (MLN)")
plt.ylabel("Number of rounds (Millions)")
#plt.yticks(np.arange(0, 1600000, step=400000), ["0.4","0.8","1.2", "1.6"])
plt.title("Number of rounds for specific round sizes")
plt.show()
plt.bar(label, amountSums)
plt.xlabel("Round amount (MLN)")
plt.ylabel("Amount invested")
plt.title("Amount invested per round size")
plt.show()

