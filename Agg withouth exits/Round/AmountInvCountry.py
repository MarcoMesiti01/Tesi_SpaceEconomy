import pandas as pd
import Library as mylib

df=pd.read_parquet("DB_Out/RoundSplit.parquet")
db_exp=pd.read_parquet("DB_Out/DB_export.parquet", columns=["company_id","company_all_tags"])
db_exp=mylib.space(db_exp)
db_exp=db_exp["company_id"]
df=df[df["Target firm ID"].isin(db_exp)]
df=df[["Firm country", "AmountUSD"]]
df["AmountUSD"]=df["AmountUSD"].apply(lambda x: x/1000000 if not pd.isna(x) else x)
df=df.groupby("Firm country")["AmountUSD"].agg(["sum", "mean", "count", "std"])
df.rename(columns={"sum" : "Total amount invested", "mean" : "Average round size", "count" : "Number of round", "std" : "Variance measure (std)"}, inplace=True)
df=df.reset_index()
fig=mylib.makeMap(df, "Total amount invested")
fig.write_html("Countries_map_amount.html")