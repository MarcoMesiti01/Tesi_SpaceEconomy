import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import Library as mylib


# Load core tables
df_updown = mylib.openDB("updown")  # index must be company_id
df_firms = mylib.openDB("export") if "company_country" in mylib.openDB("export").columns else mylib.openDB("rounds")

# Prefer a canonical source for company country
country_col = "company_country"
if country_col not in df_firms.columns:
    raise KeyError(f"Expected column '{country_col}' in firms/export or rounds table")

# Filter to space companies and keep valid upstream/downstream flags
needed_ud = ["Upstream", "Downstream", "Space"]
missing_ud = [c for c in needed_ud if c not in df_updown.columns]
if missing_ud:
    raise KeyError(f"Missing columns in updown table: {missing_ud}")

df_updown = df_updown[df_updown["Space"] == 1][["Upstream", "Downstream"]]

# Bring in country per company
df_comp = df_firms[["company_id", country_col]].dropna(subset=["company_id"]).copy()
df_comp = pd.merge(df_comp, df_updown, left_on="company_id", right_index=True, how="inner")

# Keep companies with at least one of Upstream/Downstream flagged
is_up = (df_comp["Upstream"].fillna(0) == 1)
is_down = (df_comp["Downstream"].fillna(0) == 1)
df_comp = df_comp[is_up | is_down]

# Count firms per country by upstream/downstream
cnt_up = (
    df_comp[is_up]
    .groupby(country_col, as_index=False)["company_id"]
    .nunique()
    .rename(columns={"company_id": "Upstream"})
)
cnt_down = (
    df_comp[is_down]
    .groupby(country_col, as_index=False)["company_id"]
    .nunique()
    .rename(columns={"company_id": "Downstream"})
)

counts = pd.merge(cnt_up, cnt_down, on=country_col, how="outer").fillna(0)
counts["Total"] = counts["Upstream"] + counts["Downstream"]

# Focus chart for readability: top 20 countries by total firms
counts.sort_values("Total", inplace=True)
chart = counts.tail(20)

# Plot grouped bars (Upstream vs Downstream) per country
x = np.arange(len(chart))
width = 0.45

fig, ax = plt.subplots(figsize=(12, 7))
ax.bar(x - width/2, chart["Upstream"], width, label="Upstream", color="#4C78A8")
ax.bar(x + width/2, chart["Downstream"], width, label="Downstream", color="#F58518")

ax.set_xticks(x)
ax.set_xticklabels(chart[country_col], rotation=60, ha="right")
ax.set_ylabel("Number of firms")
ax.set_title("Upstream and Downstream Firms per Country (Top 20)")
ax.legend()
plt.tight_layout()
plt.show()
