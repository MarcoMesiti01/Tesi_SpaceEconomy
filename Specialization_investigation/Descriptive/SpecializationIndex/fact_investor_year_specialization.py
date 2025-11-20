"""
Rebuild the FactInvestorYearSpecialization table with a rolling specialization index.

Each yearly specialization score reflects the share of capital invested in
space-tagged rounds over the preceding five calendar years (exclusive of the
current year). Calculations start in 2006 so every point leverages a full
five-year history (2001-2005 for 2006, ..., 2020-2024 for 2025) while the raw
round history spans 2000-2025.
"""

import sys
from pathlib import Path
from typing import Sequence

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

import Library as mylib

MIN_YEAR = 2000
MAX_YEAR = 2025
START_YEAR = 2006  # first year with a complete five-year lookback
LOOKBACK_YEARS = 5
YEARS: Sequence[int] = tuple(range(MIN_YEAR, MAX_YEAR + 1))
OUTPUT_PATH = PROJECT_ROOT / "DB_Out" / "Fact" / "FactInvestorYearSpecialization.parquet"


def _coerce_investor_ids(series: pd.Series) -> pd.Series:
    """Ensure investor ids are nullable integers."""
    coerced = pd.to_numeric(series, errors="coerce")
    return coerced.astype("Int64")


def _load_original_vc_ids() -> pd.Index:
    investors = mylib.openDB("investors")
    if "investor_id" not in investors.columns:
        raise KeyError("Column 'investor_id' is missing in the investors table.")

    investors = investors[["investor_id"]].copy()
    investors["investor_id"] = _coerce_investor_ids(investors["investor_id"])
    investors = investors.dropna(subset=["investor_id"])

    filtered = mylib.isOriginalVC(investors, True)
    ids = _coerce_investor_ids(filtered["investor_id"]).dropna().unique()
    return pd.Index(sorted(ids.tolist()), name="investor_id")


def _prepare_rounds(valid_ids: pd.Index, years: Sequence[int]) -> pd.DataFrame:
    rounds = mylib.openDB("rounds")
    required = ["investor_id", "company_id", "round_amount_usd", "round_date"]
    missing = [col for col in required if col not in rounds.columns]
    if missing:
        raise KeyError(f"Missing columns in rounds table: {missing}")

    rounds = rounds[required].copy()
    rounds["investor_id"] = _coerce_investor_ids(rounds["investor_id"])
    rounds = rounds.dropna(subset=["investor_id"])
    rounds = rounds[rounds["investor_id"].isin(valid_ids)]

    rounds["round_date"] = pd.to_datetime(rounds["round_date"], errors="coerce")
    rounds = rounds.dropna(subset=["round_date"])
    rounds["year"] = rounds["round_date"].dt.year

    start, end = min(years), max(years)
    rounds = rounds[(rounds["year"] >= start) & (rounds["year"] <= end)]

    rounds["round_amount_usd"] = pd.to_numeric(
        rounds["round_amount_usd"], errors="coerce"
    ).fillna(0.0)

    rounds = mylib.space(rounds, "company_id", False)
    space_mask = rounds["space"].fillna(0).eq(1)
    rounds["space_amount"] = rounds["round_amount_usd"].where(space_mask, 0.0)
    return rounds


def _compute_specialization(
    rounds: pd.DataFrame, valid_ids: pd.Index, years: Sequence[int]
) -> pd.DataFrame:
    if rounds.empty:
        return pd.DataFrame(0.0, index=valid_ids, columns=years)

    grouped = (
        rounds.groupby(["investor_id", "year"])
        .agg(
            total_amount=("round_amount_usd", "sum"),
            space_amount=("space_amount", "sum"),
        )
        .sort_index()
    )

    full_index = pd.MultiIndex.from_product(
        [valid_ids, years], names=["investor_id", "year"]
    )
    grouped = grouped.reindex(full_index, fill_value=0.0).reset_index()

    def apply_window(df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("year")
        df["total_window"] = (
            df["total_amount"]
            .shift(1)
            .rolling(window=LOOKBACK_YEARS, min_periods=LOOKBACK_YEARS)
            .sum()
        )
        df["space_window"] = (
            df["space_amount"]
            .shift(1)
            .rolling(window=LOOKBACK_YEARS, min_periods=LOOKBACK_YEARS)
            .sum()
        )
        return df

    grouped = grouped.groupby("investor_id", group_keys=False).apply(apply_window)
    grouped["specialization_index"] = 0.0

    valid_window = (
        (grouped["year"] >= START_YEAR) & grouped["total_window"].gt(0)
    )
    grouped.loc[valid_window, "specialization_index"] = (
        grouped.loc[valid_window, "space_window"]
        .div(grouped.loc[valid_window, "total_window"])
        .clip(0.0, 1.0)
    )

    pivot = (
        grouped.pivot(index="investor_id", columns="year", values="specialization_index")
        .reindex(columns=years, fill_value=0.0)
        .reindex(valid_ids, fill_value=0.0)
    )
    pivot.index.name = "investor_id"
    return pivot


def build_fact_table(years: Sequence[int] = YEARS) -> pd.DataFrame:
    valid_ids = _load_original_vc_ids()
    rounds = _prepare_rounds(valid_ids, years)
    return _compute_specialization(rounds, valid_ids, years)


def main() -> None:
    fact_df = build_fact_table()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fact_df.to_parquet(OUTPUT_PATH)
    print(
        f"Saved specialization index for {fact_df.shape[0]} investors "
        f"across {fact_df.shape[1]} years -> {OUTPUT_PATH}"
    )


if __name__ == "__main__":
    main()
