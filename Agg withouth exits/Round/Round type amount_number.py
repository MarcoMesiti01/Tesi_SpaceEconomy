import pandas as pd
import Library as mylib
import matplotlib.pyplot as plt

# Increase default font sizes for readability
plt.rcParams.update({
    'font.size': 14,
    'axes.titlesize': 18,
    'axes.labelsize': 14,
    'xtick.labelsize': 12,
    'ytick.labelsize': 12,
    'legend.fontsize': 12,
})


df_merge = mylib.openDB("rounds")
#df_merge=mylib.space(df_merge, "company_id", True)
df_rt_og = df_merge[["round_label", "round_amount_usd"]]
df_rt_og=mylib.filterExits(df_rt_og)
print(df_rt_og.columns)
print(df_rt_og)
df_rt_og["round_amount_usd"] = pd.to_numeric(df_rt_og["round_amount_usd"], errors="coerce")
df_rt_og["round_amount_usd"]=df_rt_og["round_amount_usd"].apply(lambda x: x/1000000000 if not pd.isna(x) else x)

# aggregate by sum
df_rt_sum = df_rt_og.groupby(by="round_label", group_keys=False).sum()
df_rt_sum.reset_index(inplace=True)
print(df_rt_sum["round_label"].to_list())
df_rt_sum = df_rt_sum[["round_label", "round_amount_usd"]]
df_rt_sum.columns = ["round_label", "sum"]
df_rt_sum.sort_values(by="sum", inplace=True)
df_rt_sum["round_label"]=df_rt_sum["round_label"].mask(df_rt_sum["round_label"] == "PROJECT, REAL ESTATE, INFRASTRUCTURE FINANCE", other="PROJ, RE, IF")
print(df_rt_sum.columns)
print(df_rt_sum[df_rt_sum["round_label"] == "PROJ, RE, IF"])

# aggregate by mean
df_rt_avg = df_rt_og.groupby(by="round_label", group_keys=False).mean()
df_rt_avg.reset_index(inplace=True)
df_rt_avg["round_label"]=df_rt_avg["round_label"].mask(df_rt_avg["round_label"] == "PROJECT, REAL ESTATE, INFRASTRUCTURE FINANCE", other="PROJ, RE, IF")
df_rt_avg = df_rt_avg[["round_label", "round_amount_usd"]]
df_rt_avg.columns = ["round_label", "average"]

# align totals and averages so the chart uses a consistent order
round_stats = df_rt_sum.merge(df_rt_avg, on="round_label", how="left")
chart_data = round_stats.tail(18)

fig, ax_sum = plt.subplots(figsize=(10, 6))
ax_sum.bar(chart_data["round_label"], chart_data["sum"], color="steelblue", label="Total amount invested")
ax_sum.set_xlabel("round_label")
ax_sum.tick_params(axis="x", rotation=90)
ax_sum.set_ylabel("Total amount invested BUSD)")
ax_sum.set_title("Amount invested and average round size per round_label")

ax_avg = ax_sum.twinx()
ax_avg.plot(chart_data["round_label"], chart_data["average"], color="darkorange", marker="o", label="Average round size")
ax_avg.set_ylabel("Average round BUSD)")

handles_sum, labels_sum = ax_sum.get_legend_handles_labels()
handles_avg, labels_avg = ax_avg.get_legend_handles_labels()
ax_sum.legend(handles_sum + handles_avg, labels_sum + labels_avg, loc="upper left")

plt.tight_layout()
plt.show()

# Visualize number of rounds using the same ordering as the amount chart
df_rt_count = (
    df_rt_og.groupby("round_label")
    .size()
    .reset_index(name="round_count")
)
df_rt_count["round_label"].mask(
    df_rt_count["round_label"] == "PROJECT, REAL ESTATE, INFRASTRUCTURE FINANCE",
    other="PROJ, RE, IF",
    inplace=True,
)
df_rt_count.sort_values(inplace=True, by="round_count")

count_chart_data = chart_data[["round_label"]].merge(
    df_rt_count, on="round_label", how="left"
)
count_chart_data["round_count"] = (
    count_chart_data["round_count"].fillna(0).astype(int)
)
count_chart_data.sort_values(by="round_count", inplace=True)

fig_count, ax_count = plt.subplots(figsize=(10, 6))
ax_count.bar(
    count_chart_data["round_label"],
    count_chart_data["round_count"],
    color="slategray",
)
ax_count.set_xlabel("round_label")
ax_count.set_ylabel("Number of rounds")
ax_count.set_title("Number of rounds per round_label")
ax_count.tick_params(axis="x", rotation=90)

plt.tight_layout()
plt.show()
