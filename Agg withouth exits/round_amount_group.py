import pandas as pd
import Library as mylib
import matplotlib.pyplot as plt

df=mylib.openDB("rounds")
db_exp=pd.read_parquet("DB_Out/DB_export.parquet", columns=["company_id","company_all_tags"])
db_exp=mylib.space(db_exp, "company_id", True)
db_exp=db_exp["company_id"]
df=df[df["Target firm ID"].isin(db_exp)]

df=df[df["AmountUSD"]!=0]
df["AmountUSD"]=df["AmountUSD"].apply(lambda x: x/1000000 if not pd.isna(x) else x)
df=mylib.filterExits(df)
df=df[df["Round type"]!="NULL"]
df_sort=df.sort_values(by="AmountUSD", ascending=False)
df_sort=df_sort[["Investor", "AmountUSD"]]
print(df_sort[:10])
df_onemln=df[df["AmountUSD"]<1]
df_threemln=df[(df["AmountUSD"]>=1) & (df["AmountUSD"]<3)]
df_fivemln=df[(df["AmountUSD"]>=3) & (df["AmountUSD"]<5)]
df_tenmln=df[(df["AmountUSD"]<10) & (df["AmountUSD"]>=5)]
df_tentwentymln=df[(df["AmountUSD"]>=10) & (df["AmountUSD"]<20)]
df_morethantwenty=df[(df["AmountUSD"]>=20) & (df["AmountUSD"]<50)]
df_morethanfifthy=df[(df["AmountUSD"]>=50) & (df["AmountUSD"]<200)]
df_morethantwohundred=df[df["AmountUSD"]>=200]

listSizes=[df_onemln.size, df_threemln.size, df_fivemln.size, df_tenmln.size, df_tentwentymln.size, df_morethantwenty.size, df_morethanfifthy.size, df_morethantwohundred.size]
print(listSizes)
label=["<1", "1-3", "3-5", "5-10", "10-20", "20-50", "50-200", ">200"]
df_onemln=df_onemln["AmountUSD"].sum()
df_threemln=df_threemln["AmountUSD"].sum()
df_fivemln=df_fivemln["AmountUSD"].sum()
df_tenmln=df_tenmln["AmountUSD"].sum()
df_tentwentymln=df_tentwentymln["AmountUSD"].sum()
df_morethantwenty=df_morethantwenty["AmountUSD"].sum()
df_morethanfifthy=df_morethanfifthy["AmountUSD"].sum()
df_morethantwohundred=df_morethantwohundred["AmountUSD"].sum()
amountSums=[df_onemln, df_threemln, df_fivemln, df_tenmln, df_tentwentymln, df_morethantwenty, df_morethanfifthy, df_morethantwohundred]
plt.bar(label, listSizes)
plt.xlabel("Round amount (MLN)")
plt.ylabel("Number of rounds")
plt.title("Number of rounds for specific round sizes (Space)")
plt.show()
plt.bar(label, amountSums)
plt.xlabel("Round amount (MLN)")
plt.ylabel("Amount invested")
plt.title("Amount invested per round size (Space)")
plt.show()

