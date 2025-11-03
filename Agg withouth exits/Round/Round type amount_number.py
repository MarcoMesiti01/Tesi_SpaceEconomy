import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

import Library as mylib

# Increase default font sizes for readability
plt.rcParams.update({
    'font.size': 14,
    'axes.titlesize': 18,
    'axes.labelsize': 14,
    'xtick.labelsize': 12,
    'ytick.labelsize': 12,
    'legend.fontsize': 12,
})


def load_round_normalization():
    """Load mapping between raw round labels and normalized round types."""
    normalization_path = (
        Path(__file__)
        .resolve()
        .parents[2]
        / "Specialization_investigation"
        / "Descriptive"
        / "Round"
        / "RoundNormaliz.JSON"
    )
    with normalization_path.open("r", encoding="utf-8") as json_file:
        round_map = json.load(json_file)
    label_to_round_type = {}
    for round_type, raw_labels in round_map.items():
        for raw_label in raw_labels:
            label_to_round_type[raw_label.strip().lower()] = round_type
    return label_to_round_type


def normalize_round_label(raw_label, label_mapping):
    if pd.isna(raw_label):
        return "Other"
    normalized_label = str(raw_label).strip().lower()
    return label_mapping.get(normalized_label, "Other")


df_merge = mylib.openDB("rounds")
df_merge=mylib.space(df_merge, "company_id", True)
df_rt_og = df_merge[["round_label", "round_amount_usd"]]
df_rt_og = mylib.filterExits(df_rt_og).copy()
print(df_rt_og.columns)
print(df_rt_og)
df_rt_og.loc[:, "round_amount_usd"] = pd.to_numeric(
    df_rt_og["round_amount_usd"], errors="coerce"
)
df_rt_og.loc[:, "round_amount_usd"] = df_rt_og["round_amount_usd"].apply(
    lambda value: value / 1_000_000_000 if not pd.isna(value) else value
)

round_label_mapping = load_round_normalization()
df_rt_og.loc[:, "round_type"] = df_rt_og["round_label"].apply(
    lambda label: normalize_round_label(label, round_label_mapping)
)
df_rt_og = df_rt_og[["round_type", "round_amount_usd"]]

# aggregate by sum
df_rt_sum = df_rt_og.groupby(by="round_type", group_keys=False).sum()
df_rt_sum.reset_index(inplace=True)
print(df_rt_sum["round_type"].to_list())
df_rt_sum = df_rt_sum[["round_type", "round_amount_usd"]]
df_rt_sum.columns = ["round_type", "sum"]
df_rt_sum.sort_values(by="sum", inplace=True)
print(df_rt_sum.columns)
print(df_rt_sum[df_rt_sum["round_type"] == "Other"])

# aggregate by mean
df_rt_avg = df_rt_og.groupby(by="round_type", group_keys=False).mean()
df_rt_avg.reset_index(inplace=True)
df_rt_avg = df_rt_avg[["round_type", "round_amount_usd"]]
df_rt_avg.columns = ["round_type", "average"]

# align totals and averages so the chart uses a consistent order
round_stats = df_rt_sum.merge(df_rt_avg, on="round_type", how="left")
chart_data = round_stats.tail(18)

fig, ax_sum = plt.subplots(figsize=(10, 6))
ax_sum.bar(chart_data["round_type"], chart_data["sum"], color="steelblue", label="Total amount invested")
ax_sum.set_xlabel("round_type")
ax_sum.tick_params(axis="x", rotation=90)
ax_sum.set_ylabel("Total amount invested BUSD)")
ax_sum.set_title("Amount invested and average round size per round_type")

ax_avg = ax_sum.twinx()
ax_avg.plot(chart_data["round_type"], chart_data["average"], color="darkorange", marker="o", label="Average round size")
ax_avg.set_ylabel("Average round BUSD)")

handles_sum, labels_sum = ax_sum.get_legend_handles_labels()
handles_avg, labels_avg = ax_avg.get_legend_handles_labels()
ax_sum.legend(handles_sum + handles_avg, labels_sum + labels_avg, loc="upper left")

plt.tight_layout()
plt.show()

# Visualize number of rounds using the same ordering as the amount chart
df_rt_count = (
    df_rt_og.groupby("round_type")
    .size()
    .reset_index(name="round_count")
)
df_rt_count.sort_values(inplace=True, by="round_count")

count_chart_data = chart_data[["round_type"]].merge(
    df_rt_count, on="round_type", how="left"
)
count_chart_data["round_count"] = (
    count_chart_data["round_count"].fillna(0).astype(int)
)
count_chart_data.sort_values(by="round_count", inplace=True)

fig_count, ax_count = plt.subplots(figsize=(10, 6))
ax_count.bar(
    count_chart_data["round_type"],
    count_chart_data["round_count"],
    color="slategray",
)
ax_count.set_xlabel("round_type")
ax_count.set_ylabel("Number of rounds")
ax_count.set_title("Number of rounds per round_type")
ax_count.tick_params(axis="x", rotation=90)

plt.tight_layout()
plt.show()
