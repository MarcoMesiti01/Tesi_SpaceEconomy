"""
Round-level specialization comparison using investor-year specialization indices.

Each round is tagged with the specialization index derived from
FactInvestorYearSpecialization (investor_id x year). Aggregations are therefore
computed directly on rounds (not investor-level averages) to capture how deal
characteristics evolve with specialization intensity.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Sequence, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[5]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

import Library as mylib  # noqa: E402  (import after sys.path tweak)

ROUND_TYPES = ["Seed", "Early Stage", "Early Growth", "Later Stage"]
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
CLASS_BINS = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0000001]
CLASS_LABELS = ["0-20%", "20%-40%", "40%-60%", "60%-80%", "80%-100%"]
FACT_TABLE_PATH = "DB_Out\Fact\FactInvestorYearSpecialization.parquet"
OUTPUT_XLSX = Path(__file__).with_name("comparison_with_not_focused_tables.xlsx")


def load_round_normalizer(script_path: Path) -> dict[str, str]:
    """Load the round label normalization dictionary if available."""
    candidates = [
        script_path.parent / "Round" / "RoundNormaliz.JSON",
        script_path.parents[1] / "Round" / "RoundNormaliz.JSON",
    ]
    json_path = next((path for path in candidates if path.exists()), None)
    if json_path is None:
        return {}

    with open(json_path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)

    mapping: dict[str, str] = {}
    for category, aliases in raw.items():
        if not aliases:
            continue
        for alias in aliases:
            key = str(alias).strip().lower()
            if key:
                mapping[key] = category
    return mapping


def mean_std(series: pd.Series, multiplier: float = 1.0) -> tuple[float, float, int]:
    """Return mean, population std, and sample size after scaling."""
    if series.empty:
        return 0.0, 0.0, 0
    cleaned = series.dropna().astype(float)
    if cleaned.empty:
        return 0.0, 0.0, 0
    scaled = cleaned * multiplier
    count = int(len(scaled))
    mean_val = float(scaled.mean())
    std_val = float(scaled.std(ddof=0)) if count > 1 else 0.0
    return mean_val, std_val, count


def normalize_country(series: pd.Series) -> pd.Series:
    """Canonical lower-case representation for domestic comparisons."""
    return series.fillna("").astype(str).str.strip().str.casefold()


def load_original_vc_ids(investors: pd.DataFrame) -> pd.Index:
    """Return investor_ids flagged as venture_capital_original."""
    if "investor_id" not in investors.columns:
        raise KeyError("investors table must include 'investor_id'")
    vc_ids = investors[["investor_id"]].dropna().copy()
    filtered = mylib.isOriginalVC(vc_ids, True)
    ids = pd.to_numeric(filtered["investor_id"], errors="coerce").dropna().unique()
    return pd.Index(sorted(ids.tolist()), name="investor_id")


def load_specialization_index() -> pd.DataFrame:
    """Read the FactInvestorYearSpecialization table and return a long dataframe."""
    fact = pd.read_parquet(FACT_TABLE_PATH)
    if fact.empty:
        raise ValueError("FactInvestorYearSpecialization table is empty.")

    fact = fact.reset_index().melt(
        id_vars="investor_id", var_name="year", value_name="specialization_index"
    )
    fact["investor_id"] = pd.to_numeric(fact["investor_id"], errors="coerce").astype("Int64")
    fact["year"] = pd.to_numeric(fact["year"], errors="coerce").astype("Int64")
    fact = fact.dropna(subset=["investor_id", "year"])
    fact["specialization_index"] = fact["specialization_index"].astype(float).clip(0.0, 1.0)
    return fact


def prepare_round_dataset() -> tuple[pd.DataFrame, list[str]]:
    """Load rounds, inject specialization index, and build round-level features."""
    investors = mylib.openDB("investors")
    rounds = mylib.openDB("rounds")
    original_ids = load_original_vc_ids(investors)

    rounds["investor_id"] = pd.to_numeric(rounds["investor_id"], errors="coerce").astype("Int64")
    rounds = rounds.dropna(subset=["investor_id"])
    rounds = rounds[rounds["investor_id"].isin(original_ids)].copy()

    rounds["round_date"] = pd.to_datetime(rounds["round_date"], errors="coerce")
    rounds = rounds.dropna(subset=["round_date"])
    rounds["year"] = rounds["round_date"].dt.year
    rounds = rounds[(rounds["year"] >= 2000) & (rounds["year"] <= 2025)].copy()

    rounds["round_amount_usd"] = pd.to_numeric(
        rounds.get("round_amount_usd"), errors="coerce"
    ).fillna(0.0)
    rounds = mylib.space(rounds, "company_id", False)
    for col in ["space", "upstream", "downstream"]:
        rounds[col] = rounds.get(col, 0).fillna(0).astype(int)

    normalizer = load_round_normalizer(Path(__file__))
    round_labels = rounds.get("round_label")
    if round_labels is not None and normalizer:
        rounds["std_round"] = round_labels.astype(str).str.strip().str.lower().map(normalizer)
    else:
        rounds["std_round"] = pd.NA

    firms = pd.read_parquet("DB_Out/DB_firms.parquet")[
        ["company_id", "company_country"]
    ]
    rounds = rounds.merge(
        firms.rename(columns={"company_country": "firm_company_country"}),
        on="company_id",
        how="left",
    )
    if "company_country" in rounds.columns:
        rounds["company_country"] = rounds["company_country"].fillna(
            rounds["firm_company_country"]
        )
    else:
        rounds["company_country"] = rounds["firm_company_country"]
    rounds = rounds.drop(columns=["firm_company_country"])

    investor_country = investors[["investor_id", "investor_country"]].copy()
    investor_country["investor_id"] = pd.to_numeric(
        investor_country["investor_id"], errors="coerce"
    ).astype("Int64")
    rounds = rounds.merge(investor_country, on="investor_id", how="left")

    rounds["domestic_flag"] = (
        normalize_country(rounds["investor_country"])
        == normalize_country(rounds["company_country"])
    ).astype(int)

    spec = load_specialization_index()
    rounds = rounds.merge(spec, on=["investor_id", "year"], how="left")
    rounds = rounds.dropna(subset=["specialization_index"])

    rounds["class"] = pd.cut(
        rounds["specialization_index"],
        bins=CLASS_BINS,
        labels=CLASS_LABELS,
        include_lowest=True,
        right=True,
    )
    rounds = rounds.dropna(subset=["class"])
    rounds["class"] = rounds["class"].astype("category")
    return rounds, CLASS_LABELS


def time_between_investments(subset: pd.DataFrame) -> tuple[float, float, int]:
    """Average months between consecutive investments inside the subset."""
    if subset.empty:
        return 0.0, 0.0, 0
    ordered = subset.sort_values(["investor_id", "round_date"])
    gaps = ordered.groupby("investor_id")["round_date"].diff().dropna()
    if gaps.empty:
        return 0.0, 0.0, 0
    months = gaps.dt.total_seconds() / (60 * 60 * 24 * 30.4375)
    return mean_std(months)


def compute_metrics_by_class(
    rounds: pd.DataFrame, class_labels: Sequence[str]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Aggregate round-level KPIs for each specialization class."""
    means = {row: [] for row in ROW_LABELS}
    stds = {row: [] for row in ROW_LABELS}
    counts = {row: [] for row in ROW_LABELS}

    for label in class_labels:
        subset = rounds[rounds["class"] == label]
        investor_count = subset["investor_id"].nunique()
        means["Number of entities"].append(float(investor_count))
        stds["Number of entities"].append(0.0)
        counts["Number of entities"].append(investor_count)

        space_rounds = subset[subset["space"] == 1]
        other_rounds = subset[subset["space"] != 1]

        for row_label, data in [
            (
                "Average round size (space firms, USD mn)",
                (space_rounds["round_amount_usd"] / 1_000_000),
            ),
            (
                "Average round size (non-space firms, USD mn)",
                (other_rounds["round_amount_usd"] / 1_000_000),
            ),
        ]:
            mean_val, std_val, count_val = mean_std(data)
            means[row_label].append(mean_val)
            stds[row_label].append(std_val)
            counts[row_label].append(count_val)

        space_company_counts = (
            space_rounds.dropna(subset=["company_id"]).groupby("company_id").size()
        )
        other_company_counts = (
            other_rounds.dropna(subset=["company_id"]).groupby("company_id").size()
        )

        for row_label, data in [
            ("Average number of rounds (space firms)", space_company_counts),
            ("Average number of rounds (non-space firms)", other_company_counts),
        ]:
            mean_val, std_val, count_val = mean_std(data)
            means[row_label].append(mean_val)
            stds[row_label].append(std_val)
            counts[row_label].append(count_val)

        mean_val, std_val, count_val = time_between_investments(subset)
        means["Average time between investments (months)"].append(mean_val)
        stds["Average time between investments (months)"].append(std_val)
        counts["Average time between investments (months)"].append(count_val)

        segment_base = space_rounds.copy()
        segment_base["other_flag"] = (
            (segment_base["upstream"] == 0) & (segment_base["downstream"] == 0)
        ).astype(int)
        for row_label, column in [
            ("Segments - % upstream", "upstream"),
            ("Segments - % downstream", "downstream"),
            ("Segments - % others", "other_flag"),
        ]:
            mean_val, std_val, count_val = mean_std(segment_base[column], multiplier=100.0)
            means[row_label].append(mean_val)
            stds[row_label].append(std_val)
            counts[row_label].append(count_val)

        valid_space = segment_base[segment_base["std_round"].isin(ROUND_TYPES)]
        valid_other = other_rounds[other_rounds["std_round"].isin(ROUND_TYPES)]

        def stage_stats(df: pd.DataFrame, stages: list[str], prefix: str) -> None:
            for stage in stages:
                indicator = (df["std_round"] == stage).astype(int)
                row_label = f"{prefix} - % {stage.lower()}"
                mean_val, std_val, count_val = mean_std(indicator, multiplier=100.0)
                means[row_label].append(mean_val)
                stds[row_label].append(std_val)
                counts[row_label].append(count_val)

        stage_stats(valid_space, ROUND_TYPES, "Round distribution (space firms)")
        stage_stats(valid_other, ROUND_TYPES, "Round distribution (non-space firms)")

        mean_val, std_val, count_val = mean_std(subset["domestic_flag"], multiplier=100.0)
        means["Domestic investments (%)"].append(mean_val)
        stds["Domestic investments (%)"].append(std_val)
        counts["Domestic investments (%)"].append(count_val)

    means_df = pd.DataFrame(means, index=class_labels).T.reindex(ROW_LABELS).fillna(0.0)
    stds_df = pd.DataFrame(stds, index=class_labels).T.reindex(ROW_LABELS).fillna(0.0)
    counts_df = (
        pd.DataFrame(counts, index=class_labels).T.reindex(ROW_LABELS).fillna(0).astype(int)
    )
    return means_df, stds_df, counts_df


def flag_significance(
    means: pd.DataFrame, stds: pd.DataFrame, counts: pd.DataFrame, alpha: float = 0.05
) -> pd.DataFrame:
    """Two-tailed z-test using the provided std/count metadata."""
    if alpha != 0.05:
        raise ValueError("Currently only alpha=0.05 is supported.")
    z_threshold = 1.96
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
                significance.loc[row, col] = mean_val != 0.0
                continue
            se = std_val
            if se == 0.0:
                significance.loc[row, col] = mean_val != 0.0
                continue
            significance.loc[row, col] = abs(mean_val) >= z_threshold * se
    return significance


def main() -> None:
    rounds, labels = prepare_round_dataset()
    means, stds, counts = compute_metrics_by_class(rounds, labels)

    significance = flag_significance(means, stds, counts, alpha=0.05)
    means_out = means.round(2)
    stds_out = stds.round(2)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        means_out.to_excel(writer, sheet_name="Means")
        stds_out.to_excel(writer, sheet_name="StdDev")
        counts.to_excel(writer, sheet_name="SampleSize")
        significance.to_excel(writer, sheet_name="Significance (5%)")

    significance_display = significance.replace({True: "Yes", False: ""})
    means_display = means_out.astype(str).applymap(lambda v: f"{float(v):.2f}")

    print("Average metrics by specialization class (* = significant at 5%):")
    print(means_display)
    print("\nStandard deviation by specialization class:")
    print(stds_out)
    print("\nSample size used for each metric (per class):")
    print(counts)
    print("\nSignificance (5% level):")
    print(significance_display)
    print(f"\nExcel output saved to: {OUTPUT_XLSX}")


if __name__ == "__main__":
    main()
