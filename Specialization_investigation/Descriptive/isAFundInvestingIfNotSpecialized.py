import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import Library as mylib
from pathlib import Path
from typing import List, Tuple


def _find_col(df: pd.DataFrame, candidates: List[str]) -> str:
    """Return the original column name matching any of `candidates`.

    - Normalizes dataframe columns by str().lower().strip() to handle non-string names
    - First tries exact matches; then substring matches in priority order
    """
    # Normalize columns to string-lower for matching
    normalized = {str(c).strip().lower(): c for c in df.columns}

    # Try exact match
    for cand in candidates:
        key = str(cand).strip().lower()
        if key in normalized:
            return normalized[key]

    # Fallback: try substring match (first hit wins, in candidate order)
    for cand in candidates:
        key = str(cand).strip().lower()
        for k, orig in normalized.items():
            if key in k:
                return orig

    # Nothing matched
    available = [str(c) for c in df.columns]
    raise KeyError(
        f"None of {candidates} matched any column. Available columns: {available}"
    )


def load_specialization_fact() -> pd.DataFrame:
    """Load FactInvestorYearSpecialization.parquet in either wide or long shape.

    Current generator (spaceSpecYear) saves a wide dataframe with:
      - index: investor_id
      - columns: years 2010..2025 (ints)
      - values: 0/1 specialization flag

    This function normalizes it to a long dataframe with columns:
      investor_id, year, is_specialized
    """
    db_dir = mylib._find_db_out_dir()
    fact_path = db_dir / "Fact" / "FactInvestorYearSpecialization.parquet"
    df = pd.read_parquet(fact_path)

    # Case 1: already long form (has year-like and a flag column)
    cols_lower = [str(c).strip().lower() for c in df.columns]
    if any(c in cols_lower for c in ("year", "yr", "fiscalyear", "calyear")) and (
        any(c in cols_lower for c in ("is_specialized", "specialized", "specialization", "space_specialized", "space_flag", "flag_space", "value"))
    ):
        inv_col = _find_col(df, ["investor_id", "investorid", "investor", "id", "investor_key", "investor_code"])
        yr_col = _find_col(df, ["year", "yr", "fiscalyear", "calyear"])
        spec_col = _find_col(df, ["is_specialized", "specialized", "specialization", "space_specialized", "space_flag", "flag_space", "value"])
        out = df[[inv_col, yr_col, spec_col]].rename(columns={inv_col: "investor_id", yr_col: "year", spec_col: "is_specialized"}).copy()
        out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
        out["is_specialized"] = pd.to_numeric(out["is_specialized"], errors="coerce").fillna(0).astype(int)
        out = out.dropna(subset=["investor_id", "year"])  # keep valid rows only
        return out

    # Case 2: wide form produced by spaceSpecYear
    # Identify investor_id from index or a column
    if df.index.name and str(df.index.name).strip().lower() == "investor_id":
        df_wide = df.copy()
        df_wide.index.name = "investor_id"
    elif "investor_id" in df.columns:
        df_wide = df.set_index("investor_id")
    else:
        # Try common variants or bail with context
        inv_col_guess = None
        for cand in ("investor", "id", "investorid", "investor_code"):
            if cand in df.columns:
                inv_col_guess = cand
                break
        if inv_col_guess is None:
            raise KeyError(f"Cannot determine investor_id in Fact table. Columns: {list(df.columns)}")
        df_wide = df.set_index(inv_col_guess)
        df_wide.index.name = "investor_id"

    # Year columns are numeric strings or ints in range 2010..2025
    def _is_year(col) -> bool:
        try:
            y = int(str(col))
            return 2010 <= y <= 2025
        except Exception:
            return False

    year_cols = [c for c in df_wide.columns if _is_year(c)]
    if not year_cols:
        raise KeyError(f"No year columns found in Fact table. Columns: {list(df_wide.columns)}")

    df_long = df_wide[year_cols].stack().reset_index()
    df_long.columns = ["investor_id", "year", "is_specialized"]
    df_long["year"] = pd.to_numeric(df_long["year"], errors="coerce").astype("Int64")
    df_long["is_specialized"] = pd.to_numeric(df_long["is_specialized"], errors="coerce").fillna(0).astype(int)
    return df_long


def investor_specialization_start(fact: pd.DataFrame) -> pd.Series:
    specialized = fact[fact["is_specialized"] == 1]
    if specialized.empty:
        return pd.Series(dtype="Int64")
    first_year = specialized.groupby("investor_id")["year"].min()
    return first_year


def investor_yearly_space_amount(rounds: pd.DataFrame) -> pd.DataFrame:
    needed = ["investor_id", "company_id", "round_date", "round_amount_usd"]
    missing = [c for c in needed if c not in rounds.columns]
    if missing:
        raise KeyError(f"Missing columns in rounds DB: {missing}")

    df = rounds[needed].copy()
    df = df.dropna(subset=["investor_id", "round_date"])  # require investor and date
    df["round_date"] = pd.to_datetime(df["round_date"], errors="coerce")
    df = df.dropna(subset=["round_date"])  # drop invalid dates
    df["year"] = df["round_date"].dt.year

    df = mylib.space(df, column="company_id", filter=False)
    df["space_amount"] = df["round_amount_usd"].fillna(0.0) * (df["Space"].fillna(0) == 1).astype(int)

    grp = (
        df.groupby(["investor_id", "year"], as_index=False)["space_amount"].sum()
    )
    return grp


def average_space_investment_around_specialization(
    rounds: pd.DataFrame,
    fact: pd.DataFrame,
    window_pre: int = 3,
    include_year_zero: bool = True,
    window_post: int = 3,
) -> Tuple[pd.DataFrame, pd.Series, pd.Series]:
    yearly = investor_yearly_space_amount(rounds)
    start_year = investor_specialization_start(fact)
    if start_year.empty:
        raise ValueError("No investors with a specialization year found in Fact table.")

    yearly = yearly.merge(start_year.rename("spec_year"), left_on="investor_id", right_index=True, how="inner")
    yearly["rel_year"] = yearly["year"] - yearly["spec_year"]

    # Build the relative year axis: [-window_pre, ..., -1, 0, 1, ..., window_post]
    rel_years = list(range(-window_pre, window_post + 1))
    if not include_year_zero:
        rel_years = [r for r in rel_years if r != 0]
    mask = yearly["rel_year"].isin(rel_years)
    yearly_window = yearly.loc[mask].copy()

    # Average across investors. We average raw amounts per (investor, rel_year), then mean across investors.
    avg_by_rel = yearly_window.groupby("rel_year")["space_amount"].mean()
    avg_by_rel = avg_by_rel.reindex(rel_years, fill_value=0.0)

    contributors = yearly_window.groupby("rel_year")["investor_id"].nunique().reindex(rel_years).fillna(0).astype(int)

    return yearly_window, avg_by_rel, contributors


def plot_pre_specialization(avg_by_rel: pd.Series, contributors: pd.Series) -> None:
    plt.figure(figsize=(8.5, 5))
    x = avg_by_rel.index.to_list()
    y = avg_by_rel.values
    plt.plot(x, y, color="#1f77b4", marker="o", linewidth=2)
    plt.axvline(0, color="#999999", linestyle="--", linewidth=1)
    plt.title("Average space investment before/after specialization")
    plt.xlabel("Years relative to specialization (0 = specialization year)")
    plt.ylabel("Average annual space investment (USD)")
    plt.grid(True, axis="y", alpha=0.25)
    for xi, yi, n in zip(x, y, contributors.values):
        plt.text(xi, yi, f"n={n}", ha="center", va="bottom", fontsize=9, color="#444444")
    plt.tight_layout()
    plt.show()


def main():
    rounds = mylib.openDB("rounds")
    fact = load_specialization_fact()

    yearly_window, avg_by_rel, contributors = average_space_investment_around_specialization(
        rounds, fact, window_pre=3, include_year_zero=True, window_post=3
    )

    print("Average space investment (USD) by relative year:")
    for rel, val in avg_by_rel.items():
        print(f"rel_year {rel:+d}: avg = {val:,.0f} USD, contributors = {contributors.loc[rel]}")

    plot_pre_specialization(avg_by_rel, contributors)


if __name__ == "__main__":
    main()
