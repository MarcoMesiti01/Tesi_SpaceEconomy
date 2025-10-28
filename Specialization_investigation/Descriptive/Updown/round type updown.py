import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import Library as mylib
from Tesi_SpaceEconomy.Specialization_investigation.flagSpaceSpec import spaceSpecialization


# Load rounds and upstream/downstream classification
df_rounds = mylib.openDB("rounds")
df_updown = mylib.openDB("updown")
dfinv=mylib.openDB("investors")
dfinv=spaceSpecialization(dfinv, 2020, 0.2)
dfinv=dfinv[dfinv["investor_flag_space"]==1]["investor_id"]
df_rounds=df_rounds[df_rounds["investor_id"].isin(dfinv)]

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
df_updown = df_updown[df_updown["Space"] == 1][["Upstream", "Downstream"]]

# Merge rounds with up/down classification (updown index is company id)
df = pd.merge(df_rounds[needed_round_cols], df_updown, how="inner", left_on="company_id", right_index=True)

# Keep only rows where upstream or downstream is defined (drop when both missing/zero)
up = (df["Upstream"].fillna(0) == 1)
down = (df["Downstream"].fillna(0) == 1)
df = df[up | down]

# Amount to numeric in billions USD for readability
df["round_amount_usd"] = pd.to_numeric(df["round_amount_usd"], errors="coerce")
df["amount_busd"] = df["round_amount_usd"].apply(lambda x: x / 1_000_000_000 if not pd.isna(x) else np.nan)

# Aggregate amounts by round type for upstream vs downstream
df_up = (
    df[up]
    .groupby("round_label", as_index=False)["amount_busd"]
    .sum()
    .rename(columns={"amount_busd": "Upstream"})
)

df_down = (
    df[down]
    .groupby("round_label", as_index=False)["amount_busd"]
    .sum()
    .rename(columns={"amount_busd": "Downstream"})
)

by_round = pd.merge(df_up, df_down, on="round_label", how="outer").fillna(0)
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
ax.set_xticklabels(chart_data["round_label"], rotation=60, ha="right")
ax.set_ylabel("Amount invested (B USD)")
ax.set_title("Upstream vs Downstream Composition by Round Type")
ax.legend()
plt.tight_layout()
plt.show()
