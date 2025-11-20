"""Distribution of space specialization ratios using the window-1518 methodology.

Specialization share = space amount / total amount over 2016-2020 (column 2021 of
FactInvestorYearSpecialization). Investors considered here must:
    1. belong to the "original VC" universe (per Library.isOriginalVC),
    2. have completed at least 4 deals overall, and
    3. post a specialization ratio >= 20%.

Outputs:
    - histogram of specialization shares,
    - average number of rounds per investor per share bin,
    - average round amount (USD mn) per share bin.
"""

from __future__ import annotations

from pathlib import Path
import sys

import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd

# Ensure we can import Library regardless of where this file sits
_CURRENT_FILE = Path(__file__).resolve()
for parent in _CURRENT_FILE.parents:
    if (parent / "Library.py").exists():
        parent_str = str(parent)
        if parent_str not in sys.path:
            sys.path.insert(0, parent_str)
        break

import Library as mylib

plt.rcParams.update({
    'font.size': 20,
    'axes.titlesize': 20,
    'axes.labelsize': 20,
    'xtick.labelsize': 20,
    'ytick.labelsize': 20,
    'legend.fontsize': 20,
})

# Analysis constants
WINDOW_REFERENCE_YEAR = 2021  # column containing the 2016-2020 specialization ratio
SPECIALIZATION_THRESHOLD = 0.20
ANALYSIS_START_YEAR = 2021
ANALYSIS_END_YEAR = 2024
HISTOGRAM_BINS = 24


def find_project_root(script_path: Path) -> Path:
    """Locate repo root (folder that hosts DB_Out)."""
    for parent in script_path.parents:
        if (parent / "DB_Out").exists():
            return parent
    return script_path.parent


def load_specialization(project_root: Path) -> pd.DataFrame:
    """Load the reference specialization column (2021) from the fact table."""
    fact_path = project_root / "DB_Out" / "Fact" / "FactInvestorYearSpecialization.parquet"
    fact = pd.read_parquet(fact_path)
    if WINDOW_REFERENCE_YEAR not in fact.columns:
        raise KeyError(
            f"Column {WINDOW_REFERENCE_YEAR} not found in {fact_path.name}. "
            f"Available columns: {list(fact.columns)}"
        )
    spec = (
        fact[[WINDOW_REFERENCE_YEAR]]
        .rename(columns={WINDOW_REFERENCE_YEAR: "space_percentage"})
        .reset_index()
    )
    spec["space_percentage"] = (
        pd.to_numeric(spec["space_percentage"], errors="coerce").fillna(0.0).clip(lower=0.0)
    )
    return spec


def filter_original_vc(investors: pd.DataFrame) -> pd.DataFrame:
    """Retain only the 'original VC' cohort using the shared helper."""
    original = mylib.isOriginalVC(investors[["investor_id"]].drop_duplicates(), True)
    original_ids = set(original["investor_id"].dropna().unique())
    return investors[investors["investor_id"].isin(original_ids)].copy()


def attach_deal_counts(investors: pd.DataFrame) -> pd.DataFrame:
    """Add the overall deal count per investor (across entire rounds table)."""
    rounds = mylib.openDB("rounds")
    counts = (
        rounds.dropna(subset=["investor_id"])
        .groupby("investor_id")
        .size()
        .rename("deal_count")
    )
    enriched = investors.merge(counts, left_on="investor_id", right_index=True, how="left")
    enriched["deal_count"] = enriched["deal_count"].fillna(0).astype(int)
    return enriched


def prepare_investor_universe(script_path: Path) -> pd.DataFrame:
    """Return the investors that satisfy the window1518 specialization criteria."""
    project_root = find_project_root(script_path)
    investors = mylib.openDB("investors")[["investor_id"]].drop_duplicates()
    investors = filter_original_vc(investors)
    if investors.empty:
        return investors

    investors = attach_deal_counts(investors)
    investors = investors[investors["deal_count"] >= 4].copy()
    if investors.empty:
        return investors

    specialization = load_specialization(project_root)
    investors = investors.merge(specialization, on="investor_id", how="left")
    investors["space_percentage"] = investors["space_percentage"].clip(0.0, 1.0)
    investors = investors[investors["space_percentage"] >= SPECIALIZATION_THRESHOLD].copy()
    investors["space_percentage"] = investors["space_percentage"].clip(0.0, 1.0)
    return investors


def configure_matplotlib() -> None:
    """Apply consistent styling for all charts."""
    mpl.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 12,
            "axes.titlesize": 16,
            "axes.labelsize": 14,
            "xtick.labelsize": 12,
            "ytick.labelsize": 12,
            "legend.fontsize": 12,
        }
    )


def plot_histogram(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Render the specialization histogram and return counts/bin edges/weights."""
    bins = HISTOGRAM_BINS
    weights = (
        np.ones(len(df["space_percentage"])) / len(df["space_percentage"])
        if len(df)
        else None
    )
    fig, ax = plt.subplots(figsize=(9.5, 5.5))
    counts, bin_edges, _ = ax.hist(
        df["space_percentage"],
        color="#1f77b4",
        edgecolor="white",
        bins=bins,
        weights=weights,
    )
    ax.set_xlabel("Percentage of space investment")
    ax.set_ylabel("Percentage of investors")
    ax.set_title("Distribution of space specialization (>=20%)")
    ax.set_xticks(np.linspace(0, 1, 6))
    ax.set_xticklabels(["0%", "20%", "40%", "60%", "80%", "100%"])
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.grid(True, axis="y", alpha=0.2)
    plt.tight_layout()
    return counts, bin_edges, weights


def compute_bin_stats(df: pd.DataFrame, bin_edges: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Compute avg deal counts and avg round amount per specialization bin."""
    rounds = mylib.openDB("rounds").copy()
    if "investor_id" not in rounds.columns or "round_date" not in rounds.columns:
        return (
            np.zeros(len(bin_edges) - 1),
            np.zeros(len(bin_edges) - 1),
            np.zeros(len(bin_edges) - 1),
        )

    rounds = rounds.dropna(subset=["investor_id", "round_date"]).copy()
    rounds["round_date"] = pd.to_datetime(rounds["round_date"], errors="coerce")
    rounds = rounds.dropna(subset=["round_date"])
    rounds["round_amount_usd"] = pd.to_numeric(rounds.get("round_amount_usd"), errors="coerce")
    rounds = mylib.space(rounds, column="company_id", filter=False)
    rounds["is_space_round"] = (rounds.get("space", 0).fillna(0) == 1).astype(int)

    rounds = rounds[
        (rounds["round_date"].dt.year >= ANALYSIS_START_YEAR)
        & (rounds["round_date"].dt.year <= ANALYSIS_END_YEAR)
    ].copy()
    rounds = rounds[rounds["investor_id"].isin(df["investor_id"].dropna())]

    deals_per_investor = rounds.groupby("investor_id").size().rename("deals_count")
    space_deals_per_investor = (
        rounds.groupby("investor_id")["is_space_round"].sum().rename("space_deals_count")
    )

    df_deals = df[["investor_id", "space_percentage"]].merge(
        deals_per_investor, left_on="investor_id", right_index=True, how="left"
    )
    df_deals = df_deals.merge(
        space_deals_per_investor, left_on="investor_id", right_index=True, how="left"
    )
    df_deals[["deals_count", "space_deals_count"]] = (
        df_deals[["deals_count", "space_deals_count"]].fillna(0).astype(float)
    )

    bins_index = pd.IntervalIndex.from_breaks(bin_edges, closed="left")
    df_deals["bin"] = pd.cut(df_deals["space_percentage"], bins=bins_index, include_lowest=True)

    deals_by_bin = (
        df_deals.groupby("bin", observed=False)["deals_count"]
        .mean()
        .reindex(bins_index, fill_value=0.0)
    )
    space_deals_by_bin = (
        df_deals.groupby("bin", observed=False)["space_deals_count"]
        .mean()
        .reindex(bins_index, fill_value=0.0)
    )

    rounds_with_pct = rounds.merge(df[["investor_id", "space_percentage"]], on="investor_id", how="inner")
    rounds_with_pct["bin"] = pd.cut(
        rounds_with_pct["space_percentage"], bins=bins_index, include_lowest=True
    )
    avg_amount_by_bin = (
        rounds_with_pct.groupby("bin", observed=False)["round_amount_usd"]
        .mean()
        .reindex(bins_index, fill_value=np.nan)
    ) / 1_000_000

    return (
        deals_by_bin.values,
        space_deals_by_bin.values,
        avg_amount_by_bin.fillna(0).values,
    )


def plot_deals(bin_centers: np.ndarray, deals_all: np.ndarray, deals_space: np.ndarray) -> None:
    """Plot average deals per investor per specialization bin."""
    fig, ax = plt.subplots(figsize=(9.5, 5.5))
    ax.set_xticks([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_xticklabels(["0%", "20%", "40%", "60%", "80%", "100%"])
    ax.set_xlim(0, 1.0)
    ax.plot(bin_centers, deals_all, color="#ff7f0e", marker="o", linewidth=2, label="Avg deals (all)")
    ax.plot(bin_centers, deals_space, color="#2ca02c", marker="s", linewidth=2, label="Avg deals (space)")
    ax.set_xlabel("Percentage of space investment")
    ax.set_ylabel("Avg deals per investor (2022-2025)")
    ax.set_title("Average deal count by specialization bin")
    ax.grid(True, axis="y", alpha=0.2)
    ax.legend()
    plt.tight_layout()


def plot_amounts(bin_centers: np.ndarray, bin_edges: np.ndarray, avg_amounts: np.ndarray) -> None:
    """Plot average round amount (USD mn) per specialization bin."""
    fig, ax = plt.subplots(figsize=(9.5, 5.5))
    ax.set_xticks([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_xticklabels(["0%", "20%", "40%", "60%", "80%", "100%"])
    ax.set_xlim(0, 1.0)
    bar_widths = np.diff(bin_edges)
    ax.bar(bin_centers, avg_amounts, width=bar_widths, color="#6baed6", edgecolor="white")
    ax.set_xlabel("Percentage of space investment")
    ax.set_ylabel("Average round amount (USD mn)")
    ax.set_title("Average round amount by specialization bin (2022-2025)")
    ax.grid(True, axis="y", alpha=0.2)
    plt.tight_layout()


def main() -> None:
    configure_matplotlib()
    investors = prepare_investor_universe(_CURRENT_FILE)
    specialized_count = len(investors)
    print(
        f"Number of specialized investors (original VC, >=4 deals, specialization >=20%): "
        f"{specialized_count}"
    )
    if investors.empty:
        print("No investors satisfy the criteria; charts skipped.")
        return

    counts, bin_edges, _ = plot_histogram(investors)
    bin_centers = 0.5 * (bin_edges[:-1] + bin_edges[1:])
    deals_all, deals_space, avg_amounts = compute_bin_stats(investors, bin_edges)
    plot_deals(bin_centers, deals_all, deals_space)
    plot_amounts(bin_centers, bin_edges, avg_amounts)

    # Display figures
    plt.show()


if __name__ == "__main__":
    main()
