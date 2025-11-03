import pandas as pd
import numpy as np
import Library as mylib


def build_investor_country_table(top_n: int = 8) -> pd.DataFrame:
    """
    Create a table with the first `top_n` countries by total amount invested (investor-based).

    Columns per country:
    - number_of_vc_funds: distinct VC investors with at least one space round
    - average_number_of_deals: VC-only mean rounds per investor
    - average_amount_invested_busd: VC-only mean total invested per investor (B USD)
    - total_amount_invested_busd: total invested by VC funds (per isOriginalVC) from that country (B USD)
    - pct_upstream: percentage of invested amount that went to upstream companies
    - pct_downstream: percentage of invested amount that went to downstream companies
    - pct_not_defined: percentage of invested amount to space companies not marked upstream/downstream
    """

    # Load DBs
    df_round = mylib.openDB("rounds")
    df_inv = mylib.openDB("investors")
    df_updown = mylib.openDB("updown")[["Upstream", "Downstream", "Space"]]

    # Keep only SPACE companies in rounds and remove exit rounds
    df_round = mylib.space(df_round, "company_id", True)
    df_round = mylib.filterExits(df_round)

    # Attach investor country and types
    df_round = pd.merge(
        df_round,
        df_inv[["investor_id", "investor_country", "investor_types"]],
        how="left",
        on="investor_id",
    )

    # Drop rows without country (cannot attribute to a country)
    df_round = df_round[df_round["investor_country"].notna()].copy()

    # Attach Upstream/Downstream flags per target company
    # updown parquet has company id as index; merge on right index
    df_round = pd.merge(
        df_round,
        df_updown[["Upstream", "Downstream"]],
        left_on="company_id",
        right_index=True,
        how="left",
    )

    # Venture capital fund flag (match token 'Venture capital' or 'venture_capital' as a list item)
    df_round["is_vc"] = df_round["investor_types"].astype(str).str.contains(
        r"(^|[,;])\s*venture[ _]?capital\s*(,|$)", case=False, regex=True, na=False
    )
    print(df_round[df_round["is_vc"]]["investor_id"].drop_duplicates().count())

    # Amount slices for up/down/other (neither up nor down)
    df_round["amount_up"] = np.where(df_round["Upstream"] == 1, df_round["round_amount_usd"], 0)
    df_round["amount_down"] = np.where(
        df_round["Downstream"] == 1, df_round["round_amount_usd"], 0
    )
    df_round["amount_other"] = np.where(
        (df_round["Upstream"] != 1) & (df_round["Downstream"] != 1),
        df_round["round_amount_usd"],
        0,
    )

    # Per-investor stats within country
    inv_stats = (
        df_round.groupby(["investor_id", "investor_country"], as_index=False)
        .agg(
            deals=("round_amount_usd", "size"),
            invested=("round_amount_usd", "sum"),
            amount_up=("amount_up", "sum"),
            amount_down=("amount_down", "sum"),
            amount_other=("amount_other", "sum"),
            is_vc=("is_vc", "max"),
        )
    )

    # Per-country aggregation (all investors for totals/shares)
    country = (
        inv_stats.groupby("investor_country", as_index=False)
        .agg(
            average_number_of_deals=("deals", "mean"),
            average_amount_invested=("invested", "mean"),
            total_amount_invested=("invested", "sum"),
            amount_up=("amount_up", "sum"),
            amount_down=("amount_down", "sum"),
            amount_other=("amount_other", "sum"),
        )
    )

    # Percentages and unit conversion to B USD (totals remain all-investor based)
    total_amt = country["total_amount_invested"].replace(0, np.nan)
    country["pct_upstream"] = (country["amount_up"] / total_amt * 100).fillna(0)
    country["pct_downstream"] = (country["amount_down"] / total_amt * 100).fillna(0)
    country["pct_not_defined"] = (country["amount_other"] / total_amt * 100).fillna(0)

    # VC-only, Europe-space-active investors: use Library.isOriginalVC to filter
    inv_stats_vc_eu = mylib.isOriginalVC(inv_stats.copy(), True)
    vc_by_country = (
        inv_stats_vc_eu.groupby("investor_country", as_index=False)
        .agg(
            number_of_vc_funds=("investor_id", "nunique"),
            vc_avg_deals=("deals", "mean"),
            vc_avg_invested=("invested", "mean"),
            vc_total_invested=("invested", "sum"),
        )
    )
    country = country.merge(vc_by_country, how="left", on="investor_country")

    # Convert monetary values to billions USD for readability
    country["average_number_of_deals"] = country["vc_avg_deals"]
    country["average_amount_invested_busd"] = country["vc_avg_invested"].fillna(0) / 1e9
    country["total_amount_invested_busd"] = country["vc_total_invested"].fillna(0) / 1e9

    # Order and select top N countries by total amount invested
    # Prepare a Europe summary row using Library.toEurope on investor_country
    # VC-only (per isOriginalVC) for first-three columns and VC total
    tmp_vc = inv_stats_vc_eu.copy()
    tmp_vc.rename(columns={"investor_country": "Country"}, inplace=True)
    tmp_vc = mylib.toEurope(tmp_vc, "Country")
    eu_vc = tmp_vc[tmp_vc["Country"] == "Europe"]
    if not eu_vc.empty:
        eu_num_vc = eu_vc["investor_id"].nunique()
        eu_avg_deals = eu_vc["deals"].mean()
        eu_avg_inv = eu_vc["invested"].mean()
        eu_total_inv = eu_vc["invested"].sum()
    else:
        eu_num_vc = 0
        eu_avg_deals = 0.0
        eu_avg_inv = 0.0
        eu_total_inv = 0.0

    # All-investor percentages aggregated to Europe using the base investor stats
    tmp_all = inv_stats.copy()
    tmp_all.rename(columns={"investor_country": "Country"}, inplace=True)
    tmp_all = mylib.toEurope(tmp_all, "Country")
    eu_all = tmp_all[tmp_all["Country"] == "Europe"]
    if not eu_all.empty:
        eu_up = eu_all["amount_up"].sum()
        eu_down = eu_all["amount_down"].sum()
        eu_other = eu_all["amount_other"].sum()
        eu_total_all = eu_all["invested"].sum()
        if eu_total_all > 0:
            eu_pct_up = eu_up / eu_total_all * 100
            eu_pct_down = eu_down / eu_total_all * 100
            eu_pct_other = eu_other / eu_total_all * 100
        else:
            eu_pct_up = eu_pct_down = eu_pct_other = 0.0
    else:
        eu_pct_up = eu_pct_down = eu_pct_other = 0.0

    # Order by VC-only total to reflect the current scope, then pick top N
    country = country.sort_values("vc_total_invested", ascending=False)
    country = country.head(top_n)

    # Final column arrangement and rounding
    country.rename(columns={"investor_country": "country"}, inplace=True)
    out_cols = [
        "country",
        "number_of_vc_funds",
        "average_number_of_deals",
        "average_amount_invested_busd",
        "total_amount_invested_busd",
        "pct_upstream",
        "pct_downstream",
        "pct_not_defined",
    ]
    country = country[out_cols].copy()

    # Build Europe row with same columns and append (always included)
    eu_row = pd.DataFrame([
        {
            "country": "Europe",
            "number_of_vc_funds": int(eu_num_vc),
            "average_number_of_deals": eu_avg_deals,
            "average_amount_invested_busd": eu_avg_inv / 1e9,
            "total_amount_invested_busd": eu_total_inv / 1e9,
            "pct_upstream": eu_pct_up,
            "pct_downstream": eu_pct_down,
            "pct_not_defined": eu_pct_other,
        }
    ])
    # Avoid duplicate EU if it appears (it shouldn't), then append
    country = pd.concat([country, eu_row], ignore_index=True)

    # Nicely rounded for presentation
    country["number_of_vc_funds"] = country["number_of_vc_funds"].fillna(0).astype(int)
    country["average_number_of_deals"] = country["average_number_of_deals"].fillna(0).round(2)
    country["average_amount_invested_busd"] = country["average_amount_invested_busd"].round(3)
    country["total_amount_invested_busd"] = country["total_amount_invested_busd"].round(3)
    country["pct_upstream"] = country["pct_upstream"].round(1)
    country["pct_downstream"] = country["pct_downstream"].round(1)
    country["pct_not_defined"] = country["pct_not_defined"].round(1)

    return country.reset_index(drop=True)


if __name__ == "__main__":
    df = build_investor_country_table(top_n=8)
    print(df)
    # Save alongside this script for convenience
    try:
        df.to_excel("InvestorCountryTable.xlsx", index=False)
    except Exception:
        # Fallback to CSV if Excel writer isn't available
        df.to_csv("InvestorCountryTable.csv", index=False)
