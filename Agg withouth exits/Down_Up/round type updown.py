import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
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


# Load rounds and upstream/downstream classification
df_rounds = mylib.openDB("rounds")
df_updown = mylib.openDB("updown")

# Ensure required columns exist
needed_round_cols = ["company_id", "round_label", "round_amount_usd"]
missing = [c for c in needed_round_cols if c not in df_rounds.columns]
if missing:
    raise KeyError(f"Missing columns in rounds table: {missing}")

needed_ud_cols = ["Upstream", "Downstream", "Space"]
missing_ud = [c for c in needed_ud_cols if c not in df_updown.columns]
if missing_ud:
    raise KeyError(f"Missing columns in updown table: {missing_ud}")

# Filter to space companies and keep only classification flags
df_updown = df_updown.loc[df_updown["Space"] == 1, ["Upstream", "Downstream"]].copy()

# Merge rounds with up/down classification (updown index is company id)
df = pd.merge(df_rounds[needed_round_cols], df_updown, how="inner", left_on="company_id", right_index=True)

# Keep only rows where upstream or downstream is defined (drop when both missing/zero)
df = df.assign(
    is_upstream=lambda data: data["Upstream"].fillna(0) == 1,
    is_downstream=lambda data: data["Downstream"].fillna(0) == 1,
)
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

# Aggregate amounts by round type for upstream vs downstream
df_up = (
    df[df["is_upstream"]]
    .groupby("round_type", as_index=False)["amount_busd"]
    .sum()
    .rename(columns={"amount_busd": "Upstream"})
)

df_down = (
    df[df["is_downstream"]]
    .groupby("round_type", as_index=False)["amount_busd"]
    .sum()
    .rename(columns={"amount_busd": "Downstream"})
)

by_round = pd.merge(df_up, df_down, on="round_type", how="outer").fillna(0)
by_round["Total"] = by_round["Upstream"] + by_round["Downstream"]

# Keep a manageable number of round types for readability
by_round.sort_values("Total", inplace=True)
chart_data = by_round.tail(15)

# Plot stacked bars: composition of upstream vs downstream by round type
fig, ax = plt.subplots(figsize=(12, 7))
x = np.arange(len(chart_data))

ax.bar(x, chart_data["Upstream"], label="Upstream", color="#4C78A8")
ax.bar(x, chart_data["Downstream"], bottom=chart_data["Upstream"], label="Downstream", color="#F58518")

ax.set_xticks(x)
ax.set_xticklabels(chart_data["round_type"], rotation=60, ha="right")
ax.set_ylabel("Amount invested (B USD)")
ax.set_title("Upstream vs Downstream Composition by Round Type")
ax.legend()
plt.tight_layout()
plt.show()
