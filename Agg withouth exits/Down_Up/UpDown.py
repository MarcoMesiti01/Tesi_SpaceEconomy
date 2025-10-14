import pandas as pd
import Library as mylib
import numpy as np
import matplotlib.pyplot as plt

df_round = pd.read_parquet("DB_Out/RoundSplit.parquet")
df_updown = pd.read_parquet("DB_Out/DB_export_updown.parquet", columns=["Target firm ID", "Upstream", "Downstream", "Tags"])
df_updown.rename(columns={"Tags":"company_all_tags"}, inplace=True)
df_updown=mylib.space(df_updown)
df_updown=df_updown[["Target firm ID", "Upstream", "Downstream"]]

df_round_updown = pd.merge(df_round, df_updown, how="left", on="Target firm ID")
df_round_updown["AmountUSD"]=df_round_updown["AmountUSD"].apply(lambda x: x/1000000000 if not pd.isna(x) else x)

df_round_up = (
    df_round_updown[df_round_updown["Upstream"] == True][["Firm country", "AmountUSD"]]
    .groupby("Firm country", as_index=False)["AmountUSD"]
    .sum()
    .rename(columns={"AmountUSD": "Amount upstream"})
)

df_round_down = (
    df_round_updown[df_round_updown["Downstream"] == True][["Firm country", "AmountUSD"]]
    .groupby("Firm country", as_index=False)["AmountUSD"]
    .sum()
    .rename(columns={"AmountUSD": "Amount downstream"})
)

df_round_updown = pd.merge(df_round_up, df_round_down, how="outer", on="Firm country").fillna(0)
df_round_updown["Total"] = df_round_updown["Amount upstream"] + df_round_updown["Amount downstream"]
df_round_updown.sort_values(by="Total", ascending=True, inplace=True)

top_countries = df_round_updown.tail(10)
x = np.arange(len(top_countries))
width = 0.4

plt.figure(figsize=(12, 7))
plt.bar(x - width / 2, top_countries["Amount upstream"], width, label="Upstream")
plt.bar(x + width / 2, top_countries["Amount downstream"], width, label="Downstream")
plt.xticks(x, top_countries["Firm country"], rotation=45, ha="right")
plt.ylabel("Investments (B USD)")
plt.title("Upstream vs Downstream Investments by Country (Top 10 by Total)")
plt.legend()
plt.tight_layout()
plt.show()
