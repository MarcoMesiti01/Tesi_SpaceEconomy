"""Quantile analysis for the original VC cohort (window1518 definition).

Filters investors to the original VC cohort with >=4 lifetime deals (no minimum
SSI cutoff) and computes quantiles over the 2016-2020 specialization index.
For each quantile (50/70/90/99), aggregates 2021-2024 investment amounts (space vs
non-space) contributed by investors at or above the threshold.
"""

from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

import Library as mylib

CURRENT_FILE = Path(__file__).resolve()
QUANTILES = [0.50, 0.70, 0.90, 0.99]
WINDOW_REFERENCE_YEAR = 2021  # specialization from 2016-2020
ANALYSIS_START_YEAR = 2021
ANALYSIS_END_YEAR = 2024


def find_project_root(script_path: Path) -> Path:
    for parent in script_path.parents:
        if (parent / "DB_Out").exists():
            return parent
    return script_path.parent


def load_specialization(project_root: Path) -> pd.DataFrame:
    fact_path = project_root / "DB_Out" / "Fact" / "FactInvestorYearSpecialization.parquet"
    fact = pd.read_parquet(fact_path)
    if WINDOW_REFERENCE_YEAR not in fact.columns:
        raise KeyError(
            f"Column {WINDOW_REFERENCE_YEAR} missing in {fact_path.name}. "
            f"Available: {list(fact.columns)}"
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


def build_investor_universe() -> pd.DataFrame:
    project_root = find_project_root(CURRENT_FILE)
    investors = mylib.openDB("investors")
    rounds = mylib.openDB("rounds")

    specialization = load_specialization(project_root)
    investors = investors.merge(specialization, on="investor_id", how="left")
    investors["space_percentage"] = investors["space_percentage"].fillna(0.0)
    if investors.empty:
        return investors

    original_vc_df = mylib.isOriginalVC(investors[["investor_id"]].drop_duplicates(), True)
    original_ids = set(original_vc_df["investor_id"].dropna().unique())
    investors = investors[investors["investor_id"].isin(original_ids)].copy()
    if investors.empty:
        return investors

    lifetime_counts = (
        rounds.dropna(subset=["investor_id"]).groupby("investor_id").size().rename("deal_count")
    )
    eligible_ids = set(lifetime_counts[lifetime_counts >= 4].index)
    investors = investors[investors["investor_id"].isin(eligible_ids)].copy()
    return investors.drop_duplicates(subset=["investor_id"])


def load_window_rounds() -> pd.DataFrame:
    rounds = mylib.openDB("rounds")
    rounds["round_date"] = pd.to_datetime(rounds["round_date"], errors="coerce")
    rounds = rounds.dropna(subset=["round_date", "investor_id"]).copy()
    rounds = rounds[
        (rounds["round_date"].dt.year >= ANALYSIS_START_YEAR)
        & (rounds["round_date"].dt.year <= ANALYSIS_END_YEAR)
    ]
    rounds = mylib.space(rounds, "company_id", False)
    rounds["round_amount_usd"] = pd.to_numeric(rounds["round_amount_usd"], errors="coerce").fillna(0.0)
    return rounds


def compute_quantiles(
    investors: pd.DataFrame,
    rounds: pd.DataFrame,
) -> pd.DataFrame:
    if investors.empty:
        raise ValueError("Investor universe is empty; cannot compute quantiles.")
    results: list[dict[str, float | int]] = []
    for quant in QUANTILES:
        threshold = float(investors["space_percentage"].quantile(quant))
        bucket = investors[investors["space_percentage"] >= threshold]
        investor_ids = set(bucket["investor_id"].dropna().unique())
        subset = rounds[rounds["investor_id"].isin(investor_ids)]

        space_amount = subset.loc[subset["space"] == 1, "round_amount_usd"].sum() / 1_000_000
        total_amount = subset["round_amount_usd"].sum() / 1_000_000
        non_space_amount = total_amount - space_amount

        results.append(
            {
                "Quantile": f"{int(quant * 100)}%",
                "Threshold (SSI)": threshold,
                "Space amount (USD mn)": space_amount,
                "Non-space amount (USD mn)": max(non_space_amount, 0.0),
                "Number of investors": len(investor_ids),
            }
        )
    return pd.DataFrame(results)


def main() -> None:
    investors = build_investor_universe()
    if investors.empty:
        print("No investors satisfy the specialization filters.")
        return
    rounds = load_window_rounds()
    quantiles_df = compute_quantiles(investors, rounds)
    investor_list = (
        investors[
            [
                "investor_id",
                "investor_name",
                "investor_country",
                "investor_city",
                "investor_types",
                "space_percentage",
            ]
        ]
        .sort_values("space_percentage", ascending=False)
    )

    output_path = Path(__file__).with_name("quantiles_specialization_window1518.xlsx")
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        quantiles_df.to_excel(writer, sheet_name="Quantiles", index=False)
        investor_list.to_excel(writer, sheet_name="Investors_by_SSI", index=False)

    print("Quantile summary (amounts in USD millions):")
    print(quantiles_df.to_string(index=False, float_format=lambda x: f"{x:.2f}"))
    print(f"\nExcel output saved to: {output_path}")


if __name__ == "__main__":
    main()
