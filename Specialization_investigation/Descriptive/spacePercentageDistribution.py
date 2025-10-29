import pandas as pd
import Library as mylib
import Tesi_SpaceEconomy.Specialization_investigation.flagSpaceSpec as flagS
import matplotlib.pyplot as plt

df=mylib.openDB("investors")

df=flagS.spacePercentage(df, 2021, 0.2)

df=df[df["space_percentage"]>0.2]

bins=25
plt.figure(figsize=(9.5, 5.5))
plt.hist(df["space_percentage"], color="#1f77b4", edgecolor="white", bins=bins)
plt.xlabel("Percentage of space investment")
plt.ylabel("Number of firms")
plt.title("Percentage distribution among space specialized firms")
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
threshold_year = 2021  # same as above call to spacePercentage
rounds = mylib.openDB("rounds").copy()

# Ensure needed columns and types
if "investor_id" in rounds.columns and "round_date" in rounds.columns:
    rounds = rounds.dropna(subset=["investor_id", "round_date"]).copy()
    rounds["round_date"] = pd.to_datetime(rounds["round_date"], errors="coerce")
    rounds = rounds.dropna(subset=["round_date"])  # drop rows with invalid dates
    #rounds=mylib.space(rounds, "company_id", True)

    # Filter to same time window [threshold_year .. 2025]
    rounds = rounds[(rounds["round_date"].dt.year >= threshold_year) & (rounds["round_date"].dt.year <= 2025)]

    # Keep only investors present in df
    if "investor_id" in df.columns:
        rounds = rounds[rounds["investor_id"].isin(df["investor_id"].dropna())]

    # Deals per investor (round count)
    deals_per_investor = rounds.groupby("investor_id").size().rename("deals_count")

    # Attach deals to investors and bin by space percentage
    df_deals = df[["investor_id", "space_percentage"]].copy()
    df_deals = df_deals.merge(deals_per_investor, left_on="investor_id", right_index=True, how="left")
    df_deals["deals_count"] = df_deals["deals_count"].fillna(0).astype(int)

    # Bin using the same edges as the histogram
    bins_index = pd.IntervalIndex.from_breaks(bin_edges, closed="left")
    df_deals["bin"] = pd.cut(df_deals["space_percentage"], bins=bins_index, include_lowest=True)

    # Average deals per bin (mean across investors in each bin)
    deals_by_bin = df_deals.groupby("bin")["deals_count"].mean().reindex(bins_index, fill_value=0)
    deals_values = deals_by_bin.values
else:
    # Fallback in case rounds schema is unexpected
    deals_values = np.zeros_like(bin_centers)

# Plot the combined figure: histogram + deals overlay on secondary axis
fig, ax1 = plt.subplots(figsize=(9.5, 5.5))
ax1.hist(df["space_percentage"], color="#1f77b4", edgecolor="white", bins=bins)
ax1.set_xlabel("Percentage of space investment")
ax1.set_ylabel("Number of Venture Capital")
ax1.set_title("Percentage distribution with avg deals per bin (space only)")
ax1.grid(True, axis="y", alpha=0.2)

ax2 = ax1.twinx()
ax2.plot(bin_centers, deals_values, color="#ff7f0e", marker="o", linewidth=2)
ax2.set_ylabel("Avg deals per investor")

plt.tight_layout()
plt.show()
