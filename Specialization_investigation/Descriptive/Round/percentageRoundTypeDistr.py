import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

import Library as mylib
from Tesi_SpaceEconomy.Specialization_investigation.flagSpaceSpec import spacePercentage


def _load_round_normalization(json_path: Path) -> dict:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Build reverse map: raw_label_lower -> standardized_category
    reverse = {}
    for std_cat, raw_list in data.items():
        for raw in raw_list:
            if raw is None:
                continue
            key = str(raw).strip().lower()
            if key:
                reverse[key] = std_cat
    return reverse


def _standardize_round_labels(df: pd.DataFrame, normalizer: dict) -> pd.Series:
    # Accept either 'round_label' or 'Round type' as the source column
    source_col = None
    for c in ("round_label", "Round type"):
        if c in df.columns:
            source_col = c
            break
    if source_col is None:
        raise KeyError("Neither 'round_label' nor 'Round type' column found in rounds DB")

    def map_label(val):
        if pd.isna(val):
            return None
        key = str(val).strip().lower()
        return normalizer.get(key)

    return df[source_col].apply(map_label)

COLOR_MAP = {
    "Seed": "#1f77b4",
    "Early Stage": "#ff7f0e",
    "Early Growth": "#2ca02c",
    "Later Stage": "#d62728",
}


def _compute_percentage_table(
    df: pd.DataFrame, labels: list[str], std_order: list[str]
) -> pd.DataFrame:
    """Return row-wise percentage table of round amounts by specialization class."""
    if df.empty:
        return pd.DataFrame(
            0.0, index=pd.Index(labels, name="class"), columns=std_order
        )

    working = df[df["class"].notna() & df["std_round"].isin(std_order)]
    if working.empty:
        return pd.DataFrame(
            0.0, index=pd.Index(labels, name="class"), columns=std_order
        )

    agg = (
        working.groupby(["class", "std_round"], as_index=False)["round_amount_usd"].sum()
    )
    pivot = (
        agg.pivot(index="class", columns="std_round", values="round_amount_usd")
        .fillna(0.0)
        .reindex(index=pd.Index(labels, name="class"), fill_value=0.0)
        .reindex(columns=std_order, fill_value=0.0)
    )

    row_totals = pivot.sum(axis=1).replace(0, pd.NA)
    pct = pivot.div(row_totals, axis=0).fillna(0.0) * 100.0
    return pct


def _annotate_stacked_bars(ax, pct: pd.DataFrame, std_order: list[str]) -> None:
    """Annotate stacked bars with percentages >= 5%."""
    for idx, x_label in enumerate(pct.index.astype(str)):
        cum = 0.0
        for col in std_order:
            val = float(pct.iloc[idx][col])
            if val >= 5:
                ax.text(
                    x_label,
                    cum + val / 2,
                    f"{val:.0f}%",
                    ha="center",
                    va="center",
                    fontsize=8,
                    color="white",
                )
            cum += val


def _plot_stacked_percentage(
    pct: pd.DataFrame, std_order: list[str], title: str
) -> None:
    """Plot a stacked percentage bar chart given the percentage table."""
    fig, ax = plt.subplots(figsize=(10, 6))
    bottom = pd.Series(0.0, index=pct.index, dtype=float)
    x_labels = pct.index.astype(str)

    for col in std_order:
        heights = pct[col].fillna(0.0).values
        ax.bar(
            x_labels,
            heights,
            bottom=bottom.values,
            label=col,
            color=COLOR_MAP.get(col, None),
        )
        bottom += pct[col].fillna(0.0)

    ax.set_xlabel("Investor specialization class (upper bound)")
    ax.set_ylabel("Share of round type (%)")
    ax.set_title(title)
    ax.legend(title="Round type", bbox_to_anchor=(1.02, 1), loc="upper left")
    ax.set_ylim(0, 100)
    ax.set_yticks(range(0, 101, 10))

    _annotate_stacked_bars(ax, pct, std_order)

    fig.tight_layout()

def main() -> None:
    # Parameters
    threshold_year = 2015  # start year for specialization window [threshold_year..2025]
    thresh_unused = 0.0    # not used by spacePercentage, kept for signature symmetry

    # Load data
    investors = mylib.openDB("investors")
    rounds = mylib.openDB("rounds")

    # Compute specialization percentage per investor
    inv_pct = spacePercentage(investors, threshold_year, thresh_unused)
    inv_pct = inv_pct[["investor_id", "space_percentage"]].dropna(subset=["investor_id"]).copy()

    # Filter exits and ensure amount is numeric
    rounds = mylib.filterExits(rounds.copy())
    if "round_amount_usd" not in rounds.columns:
        raise KeyError("rounds DB must contain column 'round_amount_usd'")
    rounds["round_amount_usd"] = pd.to_numeric(rounds["round_amount_usd"], errors="coerce").fillna(0.0)

    # Align time window with specialization computation when possible
    if "round_date" in rounds.columns:
        rounds["round_date"] = pd.to_datetime(rounds["round_date"], errors="coerce")
        rounds = rounds.dropna(subset=["round_date"])  # keep valid dates only
        rounds = rounds[(rounds["round_date"].dt.year >= threshold_year) & (rounds["round_date"].dt.year <= 2025)]

    # Join space percentage onto rounds by investor_id
    if "investor_id" not in rounds.columns:
        raise KeyError("rounds DB must contain column 'investor_id'")
    rounds = rounds.dropna(subset=["investor_id"]).copy()
    rounds = rounds.merge(inv_pct, on="investor_id", how="left")

    # Tag rounds related to space companies
    if "company_id" not in rounds.columns:
        raise KeyError("rounds DB must contain column 'company_id' for space mapping")
    rounds = mylib.space(rounds, column="company_id", filter=False)

    # Bin investors into 5 classes by specialization percentage
    edges = [0, 0.2, 0.4, 0.6, 0.8, 1.0000001]
    labels = ["0-20%", "20%-40%", "40%-60%", "60%-80%", "80%-100%"]
    rounds["class"] = pd.cut(
        rounds["space_percentage"].fillna(0.0),
        bins=edges,
        labels=labels,
        include_lowest=True,
        right=True,
    )

    # Normalize round labels to 4 standardized categories
    json_path = Path(__file__).with_name("RoundNormaliz.JSON")
    normalizer = _load_round_normalization(json_path)
    rounds["std_round"] = _standardize_round_labels(rounds, normalizer)

    # Keep only mapped categories and valid classes
    std_order = ["Seed", "Early Stage", "Early Growth", "Later Stage"]
    rounds = rounds[rounds["std_round"].isin(std_order) & rounds["class"].notna()].copy()

    if rounds.empty:
        print("No data available after mapping and filtering. Nothing to plot.")
        return
    pct_all = _compute_percentage_table(rounds, labels, std_order)
    _plot_stacked_percentage(pct_all, std_order, "Round type distribution by specialization class (amount share)")

    space_rounds = rounds[rounds["Space"].fillna(0) == 1].copy()
    if space_rounds.empty:
        print("No space-related rounds available for space-only plot.")
    else:
        pct_space = _compute_percentage_table(space_rounds, labels, std_order)
        _plot_stacked_percentage(
            pct_space,
            std_order,
            "Round type distribution by specialization class (space rounds only)",
        )

    plt.show()


if __name__ == "__main__":
    main()
