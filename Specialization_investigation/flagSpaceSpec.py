import pandas as pd
import Library as mylib
from pathlib import Path
from typing import Optional, Literal

investor = mylib.openDB("investors")

def spaceSpecialization(df_investor: pd.DataFrame, threshold_year: int, threshold_percentage: float) -> pd.DataFrame:
    """
    Adds a flag to the investor dataset. 
    returns the information investor_flag_space and investor_flag_venture_capital
    
    """
    # Validate inputs
    if threshold_percentage > 1 or threshold_percentage < 0:
        raise Exception(f"threshold must be between 0 and 1 it was: {threshold_percentage}")

    if "investor_id" not in df_investor.columns:
        raise KeyError("df_investor must contain column 'investor_id'")

    # Venture capital flag (case-insensitive; matches 'venture capital' or 'venture_capital')
    vc_pattern = r"\bventure[_ ]?capital\b"
    if "investor_types" in df_investor.columns:
        types_series = df_investor["investor_types"].astype(str)
    else:
        inv_db = mylib.openDB("investors")
        if "investor_types" not in inv_db.columns:
            raise KeyError("investors DB must contain column 'investor_types'")
        types_map = inv_db.set_index("investor_id")["investor_types"].astype(str)
        types_series = df_investor["investor_id"].map(types_map).fillna("")

    df_investor = df_investor.copy()
    df_investor["investor_flag_venture_capital"] = (
        types_series.str.contains(vc_pattern, case=False, regex=True, na=False).astype(int)
    )

    # Exclude investors with fewer than 4 total rounds (overall, not window-limited)
    rounds_all_for_filter = mylib.openDB("rounds")
    if "investor_id" not in rounds_all_for_filter.columns:
        raise KeyError("rounds DB must contain column 'investor_id'")
    counts_all = (
        rounds_all_for_filter.dropna(subset=["investor_id"]).groupby("investor_id").size().rename("rounds_count")
    )
    eligible_ids = set(counts_all[counts_all >= 4].index)

    #investor having sustained a round in european companies
    firm=pd.read_parquet("DB_Out/DB_firms.parquet")
    rounds2=mylib.openDB("rounds")
    firmEu=firm[firm["company_continent"]=="Europe"]["company_id"]
    rounds2=mylib.space(rounds2, "company_id", False)
    firmEu=firm[firm["company_continent"]=="Europe"]["company_id"] 
    roundsLen=rounds2[(rounds2["Space"]==1) & (rounds2["company_id"].isin(firmEu))]["investor_id"].drop_duplicates()
    ids=set(roundsLen)
    eligible_ids=eligible_ids.intersection(ids)
    df_investor = df_investor[df_investor["investor_id"].isin(eligible_ids)]

    # Simplified specialization: consider window [threshold_year .. 2025] inclusive
    start_year = int(threshold_year)
    end_year = 2025

    rounds = mylib.openDB("rounds")
    needed_cols = ["company_id", "investor_id", "round_date", "round_amount_usd"]
    missing = [c for c in needed_cols if c not in rounds.columns]
    if missing:
        raise KeyError(f"Missing columns in rounds table: {missing}")

    rounds = rounds[needed_cols].copy()
    rounds["round_amount_usd"] = pd.to_numeric(rounds["round_amount_usd"], errors="coerce").fillna(0.0)
    rounds["round_date"] = pd.to_datetime(rounds["round_date"], errors="coerce")
    rounds = rounds.dropna(subset=["investor_id", "round_date"])  # cannot use rows missing investor or date

    # Restrict to [start_year .. 2025]
    rounds = rounds[(rounds["round_date"].dt.year >= start_year) & (rounds["round_date"].dt.year < end_year)]

    # Add space flag and compute space amounts
    rounds = mylib.space(rounds, column="company_id", filter=False)
    rounds["space_amount"] = rounds["round_amount_usd"] * (rounds["Space"].fillna(0) == 1).astype(int)

    # Restrict to investors provided in df_investor
    inv_ids = set(df_investor["investor_id"].dropna().unique())
    if inv_ids:
        rounds = rounds[rounds["investor_id"].isin(inv_ids)]

    # Aggregate totals for the window
    agg = (
        rounds.groupby("investor_id")
        .agg(total_window=("round_amount_usd", "sum"), space_window=("space_amount", "sum"))
    )

    # Compute specialization flag: ratio >= threshold (0 when total is 0)
    agg["ratio"] = agg.apply(lambda r: 0.0 if r["total_window"] == 0 else r["space_window"] / r["total_window"], axis=1)
    agg["investor_flag_space"] = (agg["ratio"] >= threshold_percentage).astype(int)

    # Merge flags back; investors without history default to 0
    df_investor = pd.merge(
        df_investor,
        agg[["investor_flag_space"]],
        how="left",
        left_on="investor_id",
        right_index=True,
    )
    df_investor["investor_flag_space"] = df_investor["investor_flag_space"].fillna(0).astype(int)

    return df_investor

def spacePercentage(df_investor: pd.DataFrame, threshold_year: int, threshold_percentage: float) -> pd.DataFrame:
    """Compute percentage of space investments over total for [threshold_year..2025].

    Accepts an investors dataframe, a starting `threshold_year`, and a `threshold_percentage`
    (not used for computation, kept for interface symmetry). Returns the input dataframe
    with a new column `space_percentage` in [0, 1], computed as:
        sum(space round_amount_usd) / sum(total round_amount_usd)
    over the inclusive window [threshold_year..2025]. Investors with no activity receive 0.

    Filter the dataframe in order to exclude investor with less than 4 deals. 
    """

    if "investor_id" not in df_investor.columns:
        raise KeyError("df_investor must contain column 'investor_id'")

    # Exclude investors with fewer than 4 total rounds (overall, not window-limited)
    rounds_all_for_filter = mylib.openDB("rounds")
    if "investor_id" not in rounds_all_for_filter.columns:
        raise KeyError("rounds DB must contain column 'investor_id'")
    counts_all = (
        rounds_all_for_filter.dropna(subset=["investor_id"]).groupby("investor_id").size().rename("rounds_count")
    )
    eligible_ids = set(counts_all[counts_all >= 4].index)

    # Venture capital filter (case-insensitive; matches 'venture capital' or 'venture_capital')
    vc_pattern = r"\bventure[_ ]?capital\b"
    if "investor_types" in df_investor.columns:
        # Filter using types from the provided dataframe
        types_series = df_investor["investor_types"].astype(str)
        mask_vc = types_series.str.contains(vc_pattern, case=False, regex=True, na=False)
        df_investor = df_investor[mask_vc]
    else:
        # Pull investor types from the investors DB and filter
        inv_db = mylib.openDB("investors")
        if "investor_types" not in inv_db.columns:
            raise KeyError("investors DB must contain column 'investor_types'")
        types_map = inv_db.set_index("investor_id")["investor_types"].astype(str)
        types_series = df_investor["investor_id"].map(types_map).fillna("")
        mask_vc = types_series.str.contains(vc_pattern, case=False, regex=True, na=False)
        df_investor = df_investor[mask_vc]

    #investor having sustained a round in european companies
    firm=pd.read_parquet("DB_Out/DB_firms.parquet")
    rounds2=mylib.openDB("rounds")
    firmEu=firm[firm["company_continent"]=="Europe"]["company_id"]
    rounds2=mylib.space(rounds2, "company_id", False)
    firmEu=firm[firm["company_continent"]=="Europe"]["company_id"] 
    roundsLen=rounds2[(rounds2["Space"]==1) & (rounds2["company_id"].isin(firmEu))]["investor_id"].drop_duplicates()
    ids=set(roundsLen)
    eligible_ids=eligible_ids.intersection(ids)

    df_investor = df_investor[df_investor["investor_id"].isin(eligible_ids)]

    start_year = int(threshold_year)
    end_year = 2025

    rounds = mylib.openDB("rounds")
    needed_cols = ["company_id", "investor_id", "round_date", "round_amount_usd"]
    missing = [c for c in needed_cols if c not in rounds.columns]
    if missing:
        raise KeyError(f"Missing columns in rounds table: {missing}")

    rounds = rounds[needed_cols].copy()
    rounds["round_amount_usd"] = pd.to_numeric(rounds["round_amount_usd"], errors="coerce").fillna(0.0)
    rounds["round_date"] = pd.to_datetime(rounds["round_date"], errors="coerce")
    rounds = rounds.dropna(subset=["investor_id", "round_date"])  # ensure usable rows

    # Restrict to [start_year .. 2025]
    rounds = rounds[(rounds["round_date"].dt.year >= start_year) & (rounds["round_date"].dt.year < end_year)]

    # Add space flag and compute space amounts
    rounds = mylib.space(rounds, column="company_id", filter=False)
    rounds["space_amount"] = rounds["round_amount_usd"] * (rounds["Space"].fillna(0) == 1).astype(int)

    # Restrict to investors provided in df_investor
    inv_ids = set(df_investor["investor_id"].dropna().unique())
    if inv_ids:
        rounds = rounds[rounds["investor_id"].isin(inv_ids)]

    # Aggregate totals for the window and compute ratio
    agg = (
        rounds.groupby("investor_id")
        .agg(total_window=("round_amount_usd", "sum"), space_window=("space_amount", "sum"))
    )

    agg["space_percentage"] = agg.apply(
        lambda r: 0.0 if r["total_window"] == 0 else r["space_window"] / r["total_window"], axis=1
    )

    # Merge back; default to 0 if missing
    df_out = pd.merge(
        df_investor.copy(),
        agg[["space_percentage"]],
        how="left",
        left_on="investor_id",
        right_index=True,
    )
    df_out["space_percentage"] = df_out["space_percentage"].fillna(0.0)

    return df_out

def spaceSpecYear(df_investor : pd.DataFrame, threshold_percentage: float) -> pd.DataFrame:
    """Return a matrix of specialization flags by year (2010..2025).

    For each investor (VC only) and year Y, the specialization ratio is
    computed over a 5-year rolling window that EXCLUDES the current year
    (i.e., years [Y-5..Y-1], clipped at 2010). Flag = 1 when the ratio of
    space amount to total amount in that window is >= threshold_percentage.
    This truncates 2025 calculations to use data up to 2024.
    """
    if threshold_percentage > 1 or threshold_percentage < 0:
        raise Exception(f"threshold must be between 0 and 1 it was: {threshold_percentage}")

    # Years to produce as columns
    start_year = 2010
    end_year = 2025  # inclusive
    years = list(range(start_year, end_year + 1))

    # Prepare the base output structure with all requested investors
    if "investor_id" not in df_investor.columns:
        raise KeyError("df_investor must contain column 'investor_id'")

    # Filter to Venture Capital investors only (case-insensitive, matches 'venture_capital' or 'Venture capital')
    vc_pattern = r"\bventure[_ ]?capital\b"
    if "investor_types" in df_investor.columns:
        inv_type_df = df_investor[["investor_id", "investor_types"]].copy()
    else:
        inv_db = mylib.openDB("investors")
        if "investor_types" not in inv_db.columns:
            raise KeyError("investors DB must contain column 'investor_types'")
        inv_type_df = inv_db[["investor_id", "investor_types"]].copy()

    inv_type_df["investor_types"] = inv_type_df["investor_types"].astype(str)
    vc_ids = inv_type_df[inv_type_df["investor_types"].str.contains(vc_pattern, case=False, regex=True, na=False)][
        "investor_id"
    ].dropna().unique()

    # Keep only investor_ids both present in the input df and matching VC filter
    base_ids = df_investor["investor_id"].dropna().unique()
    vc_set = set(vc_ids)
    filtered_ids = [iid for iid in base_ids if iid in vc_set]

    # Now exclude investors with fewer than 4 total rounds (overall, not window-limited)
    rounds_all_for_filter = mylib.openDB("rounds")
    if "investor_id" not in rounds_all_for_filter.columns:
        raise KeyError("rounds DB must contain column 'investor_id'")
    counts_all = (
        rounds_all_for_filter.dropna(subset=["investor_id"]).groupby("investor_id").size().rename("rounds_count")
    )
    eligible_ids = set(counts_all[counts_all >= 4].index)

    #investor having sustained a round in european companies
    firm=pd.read_parquet("DB_Out/DB_firms.parquet")
    rounds2=mylib.openDB("rounds")
    firmEu=firm[firm["company_continent"]=="Europe"]["company_id"]
    rounds2=mylib.space(rounds2, "company_id", False)
    firmEu=firm[firm["company_continent"]=="Europe"]["company_id"] 
    roundsLen=rounds2[(rounds2["Space"]==1) & (rounds2["company_id"].isin(firmEu))]["investor_id"].drop_duplicates()
    ids=set(roundsLen)
    eligible_ids=eligible_ids.intersection(ids)

    filtered_ids = [iid for iid in filtered_ids if iid in eligible_ids]
    investor_ids = pd.Index(filtered_ids, name="investor_id")

    # Load rounds and enrich with space flag
    rounds = mylib.openDB("rounds")
    # Keep only columns we need; tolerate missing by using .get
    needed_cols = ["company_id", "investor_id", "round_date", "round_amount_usd"]
    missing = [c for c in needed_cols if c not in rounds.columns]
    if missing:
        raise KeyError(f"Missing columns in rounds table: {missing}")

    rounds = rounds[needed_cols].copy()

    # Ensure types
    rounds["round_amount_usd"] = pd.to_numeric(rounds["round_amount_usd"], errors="coerce").fillna(0.0)
    rounds["round_date"] = pd.to_datetime(rounds["round_date"], errors="coerce")

    # Drop rows without investor_id or date since they cannot be assigned
    rounds = rounds.dropna(subset=["investor_id", "round_date"])  # investor_id can be float; keep as-is

    # Consider the time span up to end_year-1 since we EXCLUDE the current year from the window
    # For a given Y, window is max(2010, Y-5)..Y-1. Thus the latest round date used is 2024.
    rounds = rounds[(rounds["round_date"].dt.year >= start_year) & (rounds["round_date"].dt.year <= end_year - 1)]

    # Add space flag by company_id
    rounds = mylib.space(rounds, column="company_id", filter=False)

    # Derive yearly totals per investor
    rounds["year"] = rounds["round_date"].dt.year
    # Amount attributed to space companies
    rounds["space_amount"] = rounds["round_amount_usd"] * (rounds["Space"].fillna(0) == 1).astype(int)

    # Restrict to investors of interest for efficiency
    if len(investor_ids) > 0:
        rounds = rounds[rounds["investor_id"].isin(investor_ids)]

    yearly = (
        rounds.groupby(["investor_id", "year"])
        .agg(total_amount=("round_amount_usd", "sum"), space_amount=("space_amount", "sum"))
        .sort_index()
    )

    # Build the result matrix initialized to 0
    result = pd.DataFrame(0, index=investor_ids, columns=years, dtype=int)

    # Compute 5-year lookback ratio (excluding current year) for each investor
    # For each investor, reindex years to full grid [2010..2025] so rolling works consistently
    full_year_index = pd.Index(years, name="year")

    # Iterate by investor group to avoid extremely complex pivot logic
    for inv_id, grp in yearly.groupby(level=0):
        # Extract this investor's yearly amounts
        s = grp.droplevel(0)
        s = s.reindex(full_year_index, fill_value=0)

        # 5-year sums excluding current year (Y-5..Y-1)
        prev5_total = s["total_amount"].shift(1).rolling(window=5, min_periods=1).sum()
        prev5_space = s["space_amount"].shift(1).rolling(window=5, min_periods=1).sum()

        # Compute ratio, handle division by zero -> 0
        with pd.option_context('mode.use_inf_as_na', True):
            ratio = prev5_space.divide(prev5_total).fillna(0.0)

        flags = (ratio >= threshold_percentage).astype(int)

        # Ensure we only write for investors requested; others are ignored
        if inv_id in result.index:
            result.loc[inv_id, years] = flags.values

    # Ensure dtype int and index name
    result.index.name = "investor_id"
    return result
