import pandas as pd
import Library as mylib
import Tesi_SpaceEconomy.Specialization_investigation.flagSpaceSpec as flagS
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.ticker as mtick
import numpy as np

df=mylib.openDB("investors")

df=flagS.spacePercentage(df, 2015, 0.2)

df=df[df["space_percentage"]>0.2]

# Global font settings: larger, consistent typography
mpl.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 12,          # base font size
    "axes.titlesize": 16,     # figure/axes titles
    "axes.labelsize": 14,     # axis labels
    "xtick.labelsize": 12,    # tick labels
    "ytick.labelsize": 12,
    "legend.fontsize": 12,
})

bins=24
plt.figure(figsize=(9.5, 5.5))
plt.hist(df["space_percentage"], color="#1f77b4", edgecolor="white", bins=bins)
plt.xlabel("Percentage of space investment")
plt.ylabel("Number of firms")
plt.title("Percentage distribution among space specialized firms")
plt.xticks(np.linspace(0, 1, 6), labels=["0%","20%","40%","60%","80%","100%"])
plt.grid(True, axis="y", alpha=0.2)
plt.tight_layout()
plt.show()

#make the second plot starting from here, dont modify the code before this line
import numpy as np

# Recreate the same histogram and overlay average deals per investor per bin
# Keep the same binning as the first plot
counts, bin_edges = np.histogram(df["space_percentage"], bins=bins)
bin_centers = 0.5 * (bin_edges[:-1] + bin_edges[1:])

# Compute number of deals per investor over the same window used for space_percentage
threshold_year = 2020  # same as above call to spacePercentage
rounds = mylib.openDB("rounds").copy()

# Ensure needed columns and types
if "investor_id" in rounds.columns and "round_date" in rounds.columns:
    rounds = rounds.dropna(subset=["investor_id", "round_date"]).copy()
    rounds["round_date"] = pd.to_datetime(rounds["round_date"], errors="coerce")
    rounds = rounds.dropna(subset=["round_date"])  # drop rows with invalid dates
    rounds["round_amount_usd"] = pd.to_numeric(rounds.get("round_amount_usd"), errors="coerce")
    # Tag space rounds without filtering out non-space
    rounds = mylib.space(rounds, column="company_id", filter=False)
    rounds["is_space_round"] = (rounds["Space"].fillna(0) == 1).astype(int)

    # Filter to same time window [threshold_year .. 2025]
    rounds = rounds[(rounds["round_date"].dt.year >= threshold_year) & (rounds["round_date"].dt.year < 2025)]

    # Keep only investors present in df
    if "investor_id" in df.columns:
        rounds = rounds[rounds["investor_id"].isin(df["investor_id"].dropna())]

    # Deals per investor (round count) and space-only deals per investor
    deals_per_investor = rounds.groupby("investor_id").size().rename("deals_count")
    space_deals_per_investor = (
        rounds.groupby("investor_id")["is_space_round"].sum().rename("space_deals_count")
    )

    # Attach deals to investors and bin by space percentage
    df_deals = df[["investor_id", "space_percentage"]].copy()
    df_deals = df_deals.merge(deals_per_investor, left_on="investor_id", right_index=True, how="left")
    df_deals = df_deals.merge(space_deals_per_investor, left_on="investor_id", right_index=True, how="left")
    df_deals[["deals_count", "space_deals_count"]] = df_deals[["deals_count", "space_deals_count"]].fillna(0).astype(int)

    # Bin using the same edges as the histogram
    bins_index = pd.IntervalIndex.from_breaks(bin_edges, closed="left")
    df_deals["bin"] = pd.cut(df_deals["space_percentage"], bins=bins_index, include_lowest=True)

    # Average deals per bin (mean across investors in each bin)
    deals_by_bin = df_deals.groupby("bin")["deals_count"].mean().reindex(bins_index, fill_value=0)
    space_deals_by_bin = df_deals.groupby("bin")["space_deals_count"].mean().reindex(bins_index, fill_value=0)
    deals_values = deals_by_bin.values
    space_deals_values = space_deals_by_bin.values

    # Average invested amount per bin (mean round amount in USD millions)
    rounds_with_pct = rounds.merge(df[["investor_id", "space_percentage"]], on="investor_id", how="inner")
    rounds_with_pct["bin"] = pd.cut(rounds_with_pct["space_percentage"], bins=bins_index, include_lowest=True)
    avg_amount_by_bin = (
        rounds_with_pct.groupby("bin")["round_amount_usd"].mean().reindex(bins_index, fill_value=np.nan) / 1_000_000
    )
    avg_amount_values = avg_amount_by_bin.fillna(0).values
else:
    # Fallback in case rounds schema is unexpected
    deals_values = np.zeros_like(bin_centers)
    space_deals_values = np.zeros_like(bin_centers)
    avg_amount_values = np.zeros_like(bin_centers)

# Second figure: average deals per investor per bin (separate plot)
fig, ax = plt.subplots(figsize=(9.5, 5.5))
ax.set_xticks([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])
ax.set_xticklabels(["0%", "20%", "40%", "60%", "80%", "100%"])
ax.set_xlim(0, 1.1)
ax.plot(bin_centers, deals_values, color="#ff7f0e", marker="o", linewidth=2, label="Avg deals (all)")
ax.plot(bin_centers, space_deals_values, color="#2ca02c", marker="s", linewidth=2, label="Avg deals (space only)")
ax.set_xlabel("Percentage of space investment")
ax.set_ylabel("Avg deals per investor")
ax.set_title("Average deals per investor by space% bin")



ax.grid(True, axis="y", alpha=0.2)
ax.legend()

plt.tight_layout()
plt.show()

# Third figure: average round amount per bin
fig_amount, ax_amount = plt.subplots(figsize=(9.5, 5.5))
ax_amount.set_xticks([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])
ax_amount.set_xticklabels(["0%", "20%", "40%", "60%", "80%", "100%"])
ax_amount.set_xlim(0, 1.1)
bar_widths = np.diff(bin_edges)
ax_amount.bar(bin_centers, avg_amount_values, width=bar_widths, color="#6baed6", edgecolor="white")
ax_amount.set_xlabel("Percentage of space investment")
ax_amount.set_ylabel("Average round amount (USD millions)")
ax_amount.set_title("Average round amount by space% bin")
ax_amount.grid(True, axis="y", alpha=0.2)

plt.tight_layout()
plt.show()
