import pandas as pd
import Library as mylib
from pathlib import Path
from typing import Optional, Literal

investor = mylib.openDB("investors")

def spaceSpecialization(df_investor: pd.DataFrame, threshold_year: int, threshold_percentage: float) -> pd.DataFrame:
    """
    Adds a flag to the investor dataset
    """
    round = mylib.openDB("rounds")
    round.fillna({"round_amount_usd":0}, inplace=True)
    upDown=mylib.openDB("updown")
    #amount invested in space by each investor
    roundSpace=pd.merge(left=round, right=upDown, left_on="company_id", right_index=True, how="inner")
    roundSpace=roundSpace[roundSpace["round_date"]>pd.to_datetime(str(threshold_year), format="%Y")]
    roundSpace=roundSpace[["investor_id", "round_amount_usd"]].copy()
    roundSpace=roundSpace.groupby(by="investor_id").sum()
    roundSpace.rename(columns={"round_amount_usd":"investor_amount_space"}, inplace=True)

    #recording the information in the investor table
    df_investor=pd.merge(left=df_investor, right=roundSpace, left_on="investor_id", right_index=True, how="left")
    df_investor.fillna({"investor_amount_space":0}, inplace=True)

    #investor_total_amount invested by each investor in total
    round=round[round["round_date"]>pd.to_datetime(str(threshold_year), format="%Y")]
    invAmount=round[["investor_id", "round_amount_usd", "investors_round"]].groupby(by="investor_id").agg({"round_amount_usd":"sum", "investors_round":"count"})
    invAmount.rename(columns={"round_amount_usd":"investor_total_amount", "investors_round":"investor_number_rounds"}, inplace=True)
    invAmount.fillna(0, inplace=True)

    #recording the information in the investor table
    df_investor=pd.merge(left=df_investor, right=invAmount, how="left", right_index=True, left_on="investor_id")
    print(df_investor)
    df_investor.fillna({"investor_total_amount" : 0, "investor_number_rounds" : 0}, inplace=True)
    df_investor["investor_flag_space"]=df_investor.apply(lambda x: 1 if x["investor_total_amount"]>0 and x["investor_amount_space"]/x["investor_total_amount"]>=threshold_percentage and x["investor_number_rounds"]>4 else 0, axis=1)
    df_investor["investor_flag_venture_capital"]=df_investor["investor_types"].apply(lambda x: 1 if "Venture capital" in x or "venture_capital" in x else 0)
    #df_investor=df_investor[(df_investor["investor_flag_space"]==1) & (df_investor["investor_flag_venture_capital"]==1) & (df_investor["Number of rounds"]>1)]
    return df_investor

def spaceSpecYear(df_investor : pd.DataFrame, threshold_percentage: float) -> pd.DataFrame:
    """The function accept a dataframe with the columns of the DB_investors and returns a dataframe having as index the investor_id and columns from 2010 to 2025 for each investor, that have the value 1 if the investor was specialised in that year and 0 if not. 
    The specialization is defined considering the 5 years before the one it is flagged
    i.e. for 2016 it will be considered from 2010 to 2015"""
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

    # Consider only the time span that can affect flags up to end_year
    # For a given Y, window is max(2010, Y-5)..(Y-1). Thus the latest round date used is 2024.
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

    # Compute 5-year lookback ratio for each investor
    # For each investor, reindex years to full grid [2010..2025] so that shifting/rolling works consistently
    full_year_index = pd.Index(years, name="year")

    # Iterate by investor group to avoid extremely complex pivot logic
    for inv_id, grp in yearly.groupby(level=0):
        # Extract this investor's yearly amounts
        s = grp.droplevel(0)
        s = s.reindex(full_year_index, fill_value=0)

        # Previous 8-year sums ending at Y-1
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
