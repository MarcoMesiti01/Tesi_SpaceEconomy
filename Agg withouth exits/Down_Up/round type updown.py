import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import Library as mylib

# Increase default font sizes for readability
plt.rcParams.update({
    'font.size': 20,
    'axes.titlesize': 20,
    'axes.labelsize': 20,
    'xtick.labelsize': 20,
    'ytick.labelsize': 20,
    'legend.fontsize': 20,
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


# Load rounds and upstream/downstream classification
df_rounds = mylib.openDB("rounds")
df_updown = mylib.openDB("updown")

# Sum deal amounts per round to undo the investor-level split present in the source table.
round_group_cols = [col for col in ("round_uuid", "company_id", "round_label", "round_date") if col in df_rounds.columns]
if not round_group_cols:
    raise KeyError("Expected at least one of 'round_uuid', 'company_id', 'round_label', 'round_date' in rounds table.")

df_rounds.loc[:, "round_amount_usd"] = pd.to_numeric(df_rounds["round_amount_usd"], errors="coerce")
df_rounds = (
    df_rounds.groupby(round_group_cols, dropna=False, as_index=False)["round_amount_usd"]
    .sum(min_count=1)
)

# Ensure required columns exist
needed_round_cols = ["company_id", "round_label", "round_amount_usd"]
missing = [c for c in needed_round_cols if c not in df_rounds.columns]
if missing:
    raise KeyError(f"Missing columns in rounds table: {missing}")

needed_ud_cols = ["company_id", "upstream", "downstream", "space"]
missing_ud = [c for c in needed_ud_cols if c not in df_updown.columns]
if missing_ud:
    raise KeyError(f"Missing columns in updown table: {missing_ud}")

# Filter to space companies and keep only classification flags
df_updown = df_updown.loc[
    df_updown["space"] == 1, ["company_id", "upstream", "downstream"]
].copy()
df_updown[["upstream", "downstream"]] = (
    df_updown[["upstream", "downstream"]]
    .apply(pd.to_numeric, errors="coerce")
    .fillna(0)
)

# Merge rounds with up/down classification using company_id
df = pd.merge(
    df_rounds[needed_round_cols],
    df_updown,
    how="inner",
    on="company_id",
)

# Keep only rows where upstream or downstream is defined (drop when both missing/zero)
df["is_upstream"] = df["upstream"] > 0
df["is_downstream"] = df["downstream"] > 0
df = df[df["is_upstream"] | df["is_downstream"]].copy()

# Amount to numeric in billions USD for readability
df.loc[:, "round_amount_usd"] = pd.to_numeric(df["round_amount_usd"], errors="coerce")
df.loc[:, "amount_busd"] = df["round_amount_usd"].apply(
    lambda value: value / 1_000_000_000 if not pd.isna(value) else np.nan
)

round_label_mapping = load_round_normalization()
df.loc[:, "round_type"] = df["round_label"].apply(
    lambda label: normalize_round_label(label, round_label_mapping)
)

# Drop generic catch-all category to focus on meaningful round types
df = df[df["round_type"] != "Other"].copy()

# Aggregate amounts by round type for upstream vs downstream
df_up = (
    df[df["is_upstream"]]
    .groupby("round_type", as_index=False)["amount_busd"]
    .sum()
    .rename(columns={"amount_busd": "upstream"})
)

df_down = (
    df[df["is_downstream"]]
    .groupby("round_type", as_index=False)["amount_busd"]
    .sum()
    .rename(columns={"amount_busd": "downstream"})
)

by_round = pd.merge(df_up, df_down, on="round_type", how="outer").fillna(0)
by_round["Total"] = by_round["upstream"] + by_round["downstream"]

# Keep a manageable number of round types for readability
by_round.sort_values("Total", ascending=False, inplace=True)
chart_data = by_round.head(15)
chart_data = chart_data.assign(
    up_pct=lambda data: np.where(data["Total"] > 0, data["upstream"] / data["Total"], 0),
    down_pct=lambda data: np.where(data["Total"] > 0, data["downstream"] / data["Total"], 0),
)

# Plot stacked bars: composition of upstream vs downstream by round type
fig, ax = plt.subplots(figsize=(7, 12))
x = np.arange(len(chart_data))

bars_up = ax.bar(x, chart_data["upstream"], label="upstream", color="#4C78A8")
bars_down = ax.bar(x, chart_data["downstream"], bottom=chart_data["upstream"], label="downstream", color="#F58518")

# Annotate each segment with its share of the total round amount
for rect, (_, row) in zip(bars_up, chart_data.iterrows()):
    if row["upstream"] > 0 and row["Total"] > 0:
        ax.text(
            rect.get_x() + rect.get_width() / 2,
            rect.get_y() + rect.get_height() / 2,
            f"{row['up_pct'] * 100:.1f}%",
            ha="center",
            va="center",
            color="white",
            fontsize=11,
            fontweight="bold",
        )

for rect, (_, row) in zip(bars_down, chart_data.iterrows()):
    if row["downstream"] > 0 and row["Total"] > 0:
        ax.text(
            rect.get_x() + rect.get_width() / 2,
            rect.get_y() + rect.get_height() / 2,
            f"{row['down_pct'] * 100:.1f}%",
            ha="center",
            va="center",
            color="black" if row["down_pct"] < 0.15 else "white",
            fontsize=11,
            fontweight="bold",
        )

ax.set_xticks(x)
ax.set_xticklabels(chart_data["round_type"], rotation=60, ha="right")
ax.set_ylabel("Amount invested (B USD)")
#ax.set_title("upstream vs downstream Composition by Round Type")
ax.legend()
plt.tight_layout()
plt.show()
plt.savefig("upDownWriting.png")
