# The script now performs a richer specialization analysis:
#  - builds mean/std tables for multiple investor metrics across specialization classes
#  - flags 5% significant deviations using simple z-tests
#  - exports means, std devs, sample sizes, and significance to Excel
#  - restricts to venture capital investors with >=4 deals and at least one European space deal
import json
from pathlib import Path

import pandas as pd

import Library as mylib
from Tesi_SpaceEconomy.Specialization_investigation.flagSpaceSpec import (
    spacePercentage,
)

# Canonical round buckets used to standardize round labels before aggregation
ROUND_TYPES = ["Seed", "Early Stage", "Early Growth", "Later Stage"]
# Explicit row ordering for the exported tables
ROW_LABELS = [
    "Number of entities",
    "Average round size (space firms, USD mn)",
    "Average round size (non-space firms, USD mn)",
    "Average number of rounds (space firms)",
    "Average number of rounds (non-space firms)",
    "Average time between investments (months)",
    "Segments - % upstream",
    "Segments - % downstream",
    "Segments - % others",
    "Round distribution (space firms) - % seed",
    "Round distribution (space firms) - % early stage",
    "Round distribution (space firms) - % early growth",
    "Round distribution (space firms) - % later stage",
    "Round distribution (non-space firms) - % seed",
    "Round distribution (non-space firms) - % early stage",
    "Round distribution (non-space firms) - % early growth",
    "Round distribution (non-space firms) - % later stage",
    "Domestic investments (%)",
]
# Mapping between row labels and investor-level metric columns
INVESTOR_METRIC_MAP = {
    "Average round size (space firms, USD mn)": "avg_size_space_musd",
    "Average round size (non-space firms, USD mn)": "avg_size_other_musd",
    "Average number of rounds (space firms)": "rounds_space_count",
    "Average number of rounds (non-space firms)": "rounds_other_count",
    "Average time between investments (months)": "avg_time_between_months",
    "Segments - % upstream": "upstream_pct",
    "Segments - % downstream": "downstream_pct",
    "Segments - % others": "other_segments_pct",
    "Round distribution (space firms) - % seed": "round_seed_pct_space",
    "Round distribution (space firms) - % early stage": "round_early_stage_pct_space",
    "Round distribution (space firms) - % early growth": "round_early_growth_pct_space",
    "Round distribution (space firms) - % later stage": "round_later_stage_pct_space",
    "Round distribution (non-space firms) - % seed": "round_seed_pct_other",
    "Round distribution (non-space firms) - % early stage": "round_early_stage_pct_other",
    "Round distribution (non-space firms) - % early growth": "round_early_growth_pct_other",
    "Round distribution (non-space firms) - % later stage": "round_later_stage_pct_other",
    "Domestic investments (%)": "domestic_pct",
}


def load_round_normalizer(script_path: Path) -> dict[str, str]:
    # Normalizes raw round labels so we can aggregate clean round distributions
    """Load the round type normalization dictionary if available."""
    json_path = script_path.parent / "Round" / "RoundNormaliz.JSON"
    if not json_path.exists():
        return {}

    with open(json_path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)

    mapping: dict[str, str] = {}
    for category, aliases in raw.items():
        if not aliases:
            continue
        for alias in aliases:
            # Keep lowercase/stripped keys to avoid downstream case mismatches
            key = str(alias).strip().lower()
            if key:
                mapping[key] = category
    return mapping


def mean_std(series: pd.Series, multiplier: float = 1.0) -> tuple[float, float, int]:
    # Utility reused across metrics to track mean, population std, and sample size
    """Return mean, std (population), and count scaled by multiplier; fall back to 0."""
    if series.empty:
        return 0.0, 0.0, 0
    cleaned = series.dropna()
    if cleaned.empty:
        return 0.0, 0.0, 0
    count = int(len(cleaned))
    # scale by the requested multiplier (millions or percentages) to keep units readable
    mean_val = float(cleaned.mean() * multiplier)
    std_val = float(cleaned.std() * multiplier) if count > 1 else 0.0
    return mean_val, std_val, count


def average_time_between_investments(rounds: pd.DataFrame) -> tuple[float, float, int]:
    # Measures pacing of investments to populate the time-between-investments row
    """Average months between consecutive investments per investor."""
    if rounds.empty or "round_date" not in rounds.columns:
        return 0.0, 0.0, 0

    ordered = rounds.sort_values(["investor_id", "round_date"])
    # Convert consecutive round gaps into months and reuse mean_std for output
    diffs = ordered.groupby("investor_id")["round_date"].diff().dropna()
    if diffs.empty:
        return 0.0, 0.0, 0

    months = diffs.dt.total_seconds() / (60 * 60 * 24 * 30.4375)
    return mean_std(months)


def average_rounds_per_investor(rounds: pd.DataFrame) -> tuple[float, float, int]:
    # Counts rounds per investor per class so we can gauge activity intensity
    """Average number of rounds per investor within the subset."""
    if rounds.empty:
        return 0.0, 0.0, 0
    counts = rounds.groupby("investor_id").size()
    if counts.empty:
        return 0.0, 0.0, 0
    # Mean/std computed across investors so the metric reflects fund-level behaviour
    avg = float(counts.mean())
    std = float(counts.std(ddof=0)) if len(counts) > 1 else 0.0
    return avg, std, int(len(counts))


def build_investor_metrics(rounds: pd.DataFrame, class_labels: list[str]) -> pd.DataFrame:
    """Compute per-investor KPIs prior to class-level aggregation."""
    investor_classes = (
        rounds[["investor_id", "class"]]
        .drop_duplicates()
        .dropna(subset=["investor_id"])
        .set_index("investor_id")
    )
    metrics = investor_classes.copy()

    space_rounds = rounds[rounds["space"] == 1]
    other_rounds = rounds[rounds["space"] != 1]

    metrics["avg_size_space_musd"] = (
        space_rounds.groupby("investor_id")["round_amount_usd"].mean() / 1_000_000
    )
    metrics["avg_size_other_musd"] = (
        other_rounds.groupby("investor_id")["round_amount_usd"].mean() / 1_000_000
    )

    metrics["rounds_space_count"] = space_rounds.groupby("investor_id").size()
    metrics["rounds_other_count"] = other_rounds.groupby("investor_id").size()

    rounds_sorted = rounds.sort_values(["investor_id", "round_date"])
    diffs = rounds_sorted.groupby("investor_id")["round_date"].diff()
    months = diffs.dt.total_seconds() / (60 * 60 * 24 * 30.4375)
    avg_time = (
        months.groupby(rounds_sorted["investor_id"]).mean().rename("avg_time_between_months")
    )
    metrics = metrics.join(avg_time, how="left")

    if not space_rounds.empty:
        seg = space_rounds.copy()
        seg["upstream"] = seg["upstream"].fillna(0)
        seg["downstream"] = seg["downstream"].fillna(0)
        upstream_pct = (
            seg.groupby("investor_id")["upstream"].mean() * 100.0
        ).rename("upstream_pct")
        downstream_pct = (
            seg.groupby("investor_id")["downstream"].mean() * 100.0
        ).rename("downstream_pct")
        other_pct = (
            ((seg["upstream"] == 0) & (seg["downstream"] == 0))
            .astype(int)
            .groupby(seg["investor_id"])
            .mean()
            * 100.0
        ).rename("other_segments_pct")
        metrics = metrics.join(upstream_pct, how="left")
        metrics = metrics.join(downstream_pct, how="left")
        metrics = metrics.join(other_pct, how="left")

    space_stage_map = {
        "Seed": "round_seed_pct_space",
        "Early Stage": "round_early_stage_pct_space",
        "Early Growth": "round_early_growth_pct_space",
        "Later Stage": "round_later_stage_pct_space",
    }
    other_stage_map = {
        "Seed": "round_seed_pct_other",
        "Early Stage": "round_early_stage_pct_other",
        "Early Growth": "round_early_growth_pct_other",
        "Later Stage": "round_later_stage_pct_other",
    }

    for subset, stage_map in [
        (space_rounds, space_stage_map),
        (other_rounds, other_stage_map),
    ]:
        valid = subset[subset["std_round"].isin(ROUND_TYPES)].copy()
        if valid.empty:
            continue
        amount_table = (
            valid.groupby(["investor_id", "std_round"])["round_amount_usd"].sum()
        ).unstack(fill_value=0)
        totals = amount_table.sum(axis=1).replace(0, pd.NA)
        share_table = amount_table.divide(totals, axis=0) * 100.0
        share_table = share_table.rename(columns=stage_map)
        for col in stage_map.values():
            if col not in share_table.columns:
                share_table[col] = pd.NA
        metrics = metrics.join(share_table[list(stage_map.values())], how="left")

    domestic_pct = (
        rounds.groupby("investor_id")["domestic_flag"].mean() * 100.0
    ).rename("domestic_pct")
    metrics = metrics.join(domestic_pct, how="left")

    for col in INVESTOR_METRIC_MAP.values():
        if col not in metrics.columns:
            metrics[col] = pd.NA

    metrics = metrics.reset_index().rename(columns={"class": "class"})
    metrics["class"] = pd.Categorical(
        metrics["class"], categories=class_labels, ordered=True
    )
    return metrics


def compute_metrics_by_class(
    investor_metrics: pd.DataFrame,
    class_labels: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Aggregate investor-level metrics by specialization class."""
    means: dict[str, list[float]] = {row: [] for row in ROW_LABELS}
    stds: dict[str, list[float]] = {row: [] for row in ROW_LABELS}
    counts: dict[str, list[int]] = {row: [] for row in ROW_LABELS}

    for label in class_labels:
        subset = investor_metrics[investor_metrics["class"] == label]
        investor_count = len(subset)
        means["Number of entities"].append(float(investor_count))
        stds["Number of entities"].append(0.0)
        counts["Number of entities"].append(investor_count)

        for row_label, column in INVESTOR_METRIC_MAP.items():
            series = subset[column]
            valid = series.dropna()
            if valid.empty:
                means[row_label].append(0.0)
                stds[row_label].append(0.0)
                counts[row_label].append(0)
            else:
                means[row_label].append(float(valid.mean()))
                stds[row_label].append(
                    float(valid.std(ddof=0)) if len(valid) > 1 else 0.0
                )
                counts[row_label].append(len(valid))

    means_df = pd.DataFrame.from_dict(means, orient="index", columns=class_labels)
    stds_df = pd.DataFrame.from_dict(stds, orient="index", columns=class_labels)
    counts_df = pd.DataFrame.from_dict(counts, orient="index", columns=class_labels)
    return means_df, stds_df, counts_df


def prepare_data() -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    # Consolidates all cleaning/enrichment steps needed before aggregation
    """Load, merge, and enrich the rounds dataset."""
    investors = mylib.openDB("investors")
    rounds_raw = mylib.openDB("rounds")

    # Use the shared helper to retain only VCs with >=4 deals and at least one
    # European space deal (across the 2015-2025 window), returning the filtered
    # investor set plus the computed space_percentage per investor.
    specialized_investors = spacePercentage(investors, 2015, 0.2).copy()
    if specialized_investors.empty:
        raise ValueError("No venture capital investors satisfied the specialization filters.")

    # Enrich rounds with space/up/down flags before applying time and geography filters
    rounds = mylib.space(rounds_raw.copy(), "company_id", False)

    # Normalize the space/upstream/downstream flags to numeric values for aggregation
    if "space" in rounds.columns:
        rounds["space"] = rounds["space"].fillna(0)
    else:
        rounds["space"] = 0
    rounds["upstream"] = rounds.get("upstream", 0).fillna(0)
    rounds["downstream"] = rounds.get("downstream", 0).fillna(0)

    # Restrict to the 2015-2025 window used for specialization calculations
    rounds["round_date"] = pd.to_datetime(rounds["round_date"], errors="coerce")
    rounds = rounds.dropna(subset=["round_date"])
    rounds = rounds[
        (rounds["round_date"].dt.year >= 2015)
        & (rounds["round_date"].dt.year <= 2025)
    ].copy()

    # Identify investors with at least one space-tagged deal in a European firm within the window
    firms = pd.read_parquet("DB_Out/DB_firms.parquet")
    if "company_continent" in firms.columns:
        continent_series = firms["company_continent"].astype(str).str.strip().str.casefold()
    else:
        continent_series = pd.Series("", index=firms.index)
    europe_company_ids = set(
        firms.loc[continent_series == "europe", "company_id"].dropna().unique()
    )
    space_europe_ids = set(
        rounds.loc[
            (rounds["space"] == 1) & (rounds["company_id"].isin(europe_company_ids)),
            "investor_id",
        ]
        .dropna()
        .unique()
    )

    specialized_investors = specialized_investors[
        specialized_investors["investor_id"].isin(space_europe_ids)
        & (specialized_investors["space_percentage"].fillna(0) > 0)
    ].copy()
    if specialized_investors.empty:
        raise ValueError(
            "No investors remain after enforcing the European space-deal requirement within the analysis window."
        )
    valid_ids = set(specialized_investors["investor_id"].dropna().unique())

    # Compute the specialization percentage per eligible investor (0..1 scale)
    inv_percentage = specialized_investors[["investor_id", "space_percentage"]]

    # Keep only rounds tied to the filtered investor universe
    rounds = rounds[rounds["investor_id"].isin(valid_ids)].copy()

    # Attach the specialization share to each round for later binning
    rounds = pd.merge(
        rounds,
        inv_percentage,
        on="investor_id",
        how="left",
    )
    rounds["space_percentage"] = rounds["space_percentage"].fillna(0.0)

    # Ensure monetary values are numeric so means/stds are stable
    rounds["round_amount_usd"] = pd.to_numeric(
        rounds["round_amount_usd"], errors="coerce"
    ).fillna(0.0)

    # Grab investor home country to compute domestic vs foreign hits
    country_cols = specialized_investors[["investor_id", "investor_country"]].drop_duplicates()
    rounds = rounds.merge(country_cols, on="investor_id", how="left")

    rounds["investor_country"] = rounds["investor_country"].fillna("").astype(str)
    if "company_country" not in rounds.columns:
        rounds["company_country"] = ""
    rounds["company_country"] = rounds["company_country"].fillna("").astype(str)
    # Domestic flag later feeds the average % domestic investment metric
    rounds["domestic_flag"] = (
        rounds["investor_country"].str.strip().str.casefold()
        == rounds["company_country"].str.strip().str.casefold()
    ).astype(int)

    # Build five even bins between 0% and 100% specialized
    edges = [0, 0.2, 0.4, 0.6, 0.8, 1.0000001]
    labels = ["0-20%", "20%-40%", "40%-60%", "60%-80%", "80%-100%"]
    # Translate the continuous specialization score into five evenly spaced classes
    rounds["class"] = pd.cut(
        rounds["space_percentage"],
        bins=edges,
        labels=labels,
        include_lowest=True,
        right=True,
    )

    normalizer = load_round_normalizer(Path(__file__))
    if normalizer:
        rounds["std_round"] = rounds["round_label"].apply(
            lambda val: normalizer.get(str(val).strip().lower()) if pd.notna(val) else None
        )
    else:
        rounds["std_round"] = pd.NA

    investor_metrics = build_investor_metrics(rounds, labels)

    # Hand back the enriched rounds table, investor metrics, and class labels
    return rounds, investor_metrics, labels


def flag_significance(
    means: pd.DataFrame,
    stds: pd.DataFrame,
    counts: pd.DataFrame,
    alpha: float = 0.05,
) -> pd.DataFrame:
    # Performs a quick z-test (sigma known approximation) using the collected std/count info
    """Return boolean mask where mean differs from zero at given alpha (two-tailed)."""
    # 5% threshold is the only scenario we calibrated; bail out otherwise
    if alpha != 0.05:
        raise ValueError("Currently only alpha=0.05 is supported")
    z_threshold = 1.96  # two-tailed 5%

    # Preallocate a boolean table mirroring the means layout
    significance = pd.DataFrame(False, index=means.index, columns=means.columns)
    for row in means.index:
        if row == "Number of entities":
            continue
        for col in means.columns:
            mean_val = float(means.loc[row, col])
            std_val = float(stds.loc[row, col])
            count_val = counts.loc[row, col]

            if pd.isna(count_val) or count_val <= 1:
                continue

            if std_val == 0.0:
                if mean_val != 0.0:
                    significance.loc[row, col] = True
                continue

            standard_error = std_val / (count_val ** 0.5)
            if standard_error == 0.0:
                if mean_val != 0.0:
                    significance.loc[row, col] = True
                continue

            # Compare absolute mean against 1.96 * standard error (two-tailed 5%)
            significance.loc[row, col] = abs(mean_val) >= z_threshold * standard_error

    return significance


def main() -> None:
    rounds, investor_metrics, labels = prepare_data()
    means, stds, counts = compute_metrics_by_class(investor_metrics, labels)

    # Align row order and fill gaps so every KPI appears in the final tables
    means = means.reindex(index=ROW_LABELS).fillna(0.0)
    stds = stds.reindex(index=ROW_LABELS).fillna(0.0)
    counts = counts.reindex(index=ROW_LABELS).fillna(0).astype(int)

    significance = flag_significance(means, stds, counts, alpha=0.05)

    # Prepare nicely formatted copies for printing/export
    means_out = means.round(2)
    stds_out = stds.round(2)
    # Keep both raw boolean mask (for Excel) and human-readable view (for console)
    significance_out = significance.copy()
    # Friendly view for console output (blank for non-significant cells)
    significance_display = significance.replace({True: "Yes", False: ""})

    means_display = means_out.astype(str)
    for row in means_display.index:
        for col in means_display.columns:
            means_display.loc[row, col] = f"{means_out.loc[row, col]:.2f}"

    # Store everything next to the script so downstream notebooks can pick it up easily
    output_path = Path(__file__).with_name("comparison_with_not_focused_tables.xlsx")
    # Send each table to its dedicated Excel sheet for downstream analysis
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        means_out.to_excel(writer, sheet_name="Means")
        stds_out.to_excel(writer, sheet_name="StdDev")
    counts.to_excel(writer, sheet_name="SampleSize")
    significance_out.to_excel(writer, sheet_name="Significance (5%)")

    # Mirror the Excel output in the terminal for quick inspection during runs
    print("Average metrics by specialization class (* = significant at 5%):")
    print(means_display)
    print("\nStandard deviation by specialization class:")
    print(stds_out.round(2))
    print("\nSample size used for each metric (per class):")
    print(counts)
    print("\nSignificance (5% level):")
    print(significance_display)
    print(f"\nExcel output saved to: {output_path}")


if __name__ == "__main__":
    # Allow running the module directly to regenerate the Excel output on demand
    main()
