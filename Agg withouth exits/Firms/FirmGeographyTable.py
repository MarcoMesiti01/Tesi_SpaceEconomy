import pandas as pd
import numpy as np
import Library as mylib


def build_firm_geography_table() -> pd.DataFrame:
    """
    Firm-based country table with the following columns per country:
    - number_of_firms: distinct firms with at least one space round (exits excluded)
    - average_amount_raised_busd: average total raised per firm (B USD)
    - average_number_of_rounds: average number of rounds per firm
    - total_amount_raised_busd: total raised by firms in that country (B USD)
    - pct_upstream: share of invested amount to upstream firms
    - pct_downstream: share of invested amount to downstream firms
    - pct_not_defined: share of invested amount to firms not flagged up/down
    """

    # Load DBs
    df_round = mylib.openDB("rounds")
    df_updown = mylib.openDB("updown")[ ["upstream", "downstream", "space"] ]

    # Keep only space companies and remove exits
    df_round = mylib.space(df_round, "company_id", True)
    df_round = mylib.filterExits(df_round)

    # Need company_country; drop rows without it
    df_round = df_round[df_round["company_country"].notna()].copy()

    # Merge upstream/downstream flags (index of updown is company_id)
    df_round = pd.merge(
        df_round,
        df_updown[["upstream", "downstream"]],
        left_on="company_id",
        right_index=True,
        how="left",
        suffixes=("", "_dup"),
    )
    df_round.drop(columns=["upstream_dup", "downstream_dup"], inplace=True, errors="ignore")

    # Per-firm stats (total raised and rounds per firm)
    firm_stats = (
        df_round.groupby(["company_id", "company_country"], as_index=False)
        .agg(
            rounds=("round_amount_usd", "size"),
            raised=("round_amount_usd", "sum"),
        )
    )

    # Country-level aggregates from firm_stats
    country_firm = (
        firm_stats.groupby("company_country", as_index=False)
        .agg(
            number_of_firms=("company_id", "nunique"),
            average_number_of_rounds=("rounds", "mean"),
            average_amount_raised=("raised", "mean"),
            total_amount_raised=("raised", "sum"),
        )
    )

    # Up/Down/Other amounts based on per-round flags
    df_round["amount_up"] = np.where(df_round["upstream"] == 1, df_round["round_amount_usd"], 0)
    df_round["amount_down"] = np.where(df_round["downstream"] == 1, df_round["round_amount_usd"], 0)
    df_round["amount_other"] = np.where(
        (df_round["upstream"] != 1) & (df_round["downstream"] != 1),
        df_round["round_amount_usd"],
        0,
    )

    by_country_amounts = (
        df_round.groupby("company_country", as_index=False)
        .agg(
            amount_up=("amount_up", "sum"),
            amount_down=("amount_down", "sum"),
            amount_other=("amount_other", "sum"),
        )
    )

    country = country_firm.merge(by_country_amounts, how="left", on="company_country")

    # Percentages and unit conversion
    total_amt = country["total_amount_raised"].replace(0, np.nan)
    country["pct_upstream"] = (country["amount_up"] / total_amt * 100).fillna(0)
    country["pct_downstream"] = (country["amount_down"] / total_amt * 100).fillna(0)
    country["pct_not_defined"] = (country["amount_other"] / total_amt * 100).fillna(0)

    # Convert monetary values to billions USD
    country["average_amount_raised_busd"] = country["average_amount_raised"] / 1e9
    country["total_amount_raised_busd"] = country["total_amount_raised"] / 1e9

    # Final column order and formatting
    country.rename(columns={"company_country": "country"}, inplace=True)
    out_cols = [
        "country",
        "number_of_firms",
        "average_amount_raised_busd",
        "average_number_of_rounds",
        "total_amount_raised_busd",
        "pct_upstream",
        "pct_downstream",
        "pct_not_defined",
    ]
    country = country[out_cols].copy()

    country["average_amount_raised_busd"] = country["average_amount_raised_busd"].round(3)
    country["average_number_of_rounds"] = country["average_number_of_rounds"].round(2)
    country["total_amount_raised_busd"] = country["total_amount_raised_busd"].round(3)
    country["pct_upstream"] = country["pct_upstream"].round(1)
    country["pct_downstream"] = country["pct_downstream"].round(1)
    country["pct_not_defined"] = country["pct_not_defined"].round(1)

    # Sort by total amount raised
    country.sort_values("total_amount_raised_busd", ascending=False, inplace=True)
    country.reset_index(drop=True, inplace=True)
    return country


if __name__ == "__main__":
    df = build_firm_geography_table()
    print(df)
    try:
        df.to_excel("FirmGeographyTable.xlsx", index=False)
    except Exception:
        df.to_csv("FirmGeographyTable.csv", index=False)
