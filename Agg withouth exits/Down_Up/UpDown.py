import pandas as pd
import Library as mylib
import numpy as np
import matplotlib.pyplot as plt

df_round = pd.read_parquet("DB_Out/DB_rounds.parquet")
df_updown = pd.read_parquet("DB_Out/DB_updown.parquet", columns=["id", "Upstream", "Downstream", "Tags_x", "Space"])
df_updown.rename(columns={"Tags_x":"company_all_tags"}, inplace=True)
df_updown=df_updown[df_updown["Space"]==1]
df_updown=df_updown[["Upstream", "Downstream"]]

df_round_updown = pd.merge(df_round, df_updown, how="left", left_on="company_id", right_on="id")
df_round_updown["round_amount_usd"]=df_round_updown["round_amount_usd"].apply(lambda x: x/1000000000 if not pd.isna(x) else x)

df_round_up = (
    df_round_updown[df_round_updown["Upstream"] == True][["company_country", "round_amount_usd"]]
    .groupby("company_country", as_index=False)["round_amount_usd"]
    .sum()
    .rename(columns={"round_amount_usd": "Amount upstream"})
)

df_round_down = (
    df_round_updown[df_round_updown["Downstream"] == True][["company_country", "round_amount_usd"]]
    .groupby("company_country", as_index=False)["round_amount_usd"]
    .sum()
    .rename(columns={"round_amount_usd": "Amount downstream"})
)

df_round_updown = pd.merge(df_round_up, df_round_down, how="outer", on="company_country").fillna(0)
df_round_updown["Total"] = df_round_updown["Amount upstream"] + df_round_updown["Amount downstream"]
df_round_updown.sort_values(by="Total", ascending=True, inplace=True)

top_countries = df_round_updown.tail(10)
x = np.arange(len(top_countries))
width = 0.4

plt.figure(figsize=(12, 7))
plt.bar(x - width / 2, top_countries["Amount upstream"], width, label="Upstream")
plt.bar(x + width / 2, top_countries["Amount downstream"], width, label="Downstream")
plt.xticks(x, top_countries["company_country"], rotation=45, ha="right")
plt.ylabel("Investments (B USD)")
plt.title("Upstream vs Downstream Investments by Country (Top 10 by Total)")
plt.legend()
plt.tight_layout()
plt.show()
